#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::catalog {

namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns,
                                         const std::string &name, const Schema &schema) {
  const table_oid_t table_oid = static_cast<table_oid_t>(next_oid_++);
  return CreateTableEntry(txn, table_oid, ns, name, schema) ? table_oid : INVALID_TABLE_OID;
}

bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns,
                                         const std::string &name);

bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name) {
  // delete the entry
  // CreateTableEntry (same oid, new name)
  // update indexes
}

bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns,
                                         const std::string &name, table_oid_t table, IndexSchema *schema);

bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns,
                                         const std::string &name);

const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

bool DatabaseCatalog::CreateTableEntry(transaction::TransactionContext *const txn, const table_oid_t table_oid,
                                       const namespace_oid_t ns_oid, const std::string &name, const Schema &schema) {
  auto [pr_init, pr_map] = classes_->InitializerForProjectedRow(PG_CLASS_ALL_COL_OIDS);

  auto *const insert_redo = txn->StageWrite(db_oid_, table_oid, pr_init);
  auto *const insert_pr = insert_redo->Delta();

  // Write the ns_oid into the PR
  const auto ns_offset = pr_map[RELNAMESPACE_COL_OID];
  auto *const ns_ptr = insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<uint32_t *>(ns_ptr)) = static_cast<uint32_t>(ns_oid);

  // Write the table_oid into the PR
  const auto table_oid_offset = pr_map[RELOID_COL_OID];
  auto *const table_oid_ptr = insert_pr->AccessForceNotNull(table_oid_offset);
  *(reinterpret_cast<uint32_t *>(table_oid_ptr)) = static_cast<uint32_t>(table_oid);

  // Write the col oids into a new Schema object
  col_oid_t next_col_oid(1);
  auto *const schema_ptr = new Schema(schema);
  // TODO(Matt): when AbstractExpressions are added to Schema::Column as a field for default
  // value, we need to make sure Column gets a properly written copy constructor to deep
  // copy those to guarantee that this copy mechanism still works
  for (auto &column : schema_ptr->columns_) {
    column.oid_ = next_col_oid++;
  }

  // Write the next_col_oid into the PR
  const auto next_col_oid_offset = pr_map[REL_NEXTCOLOID_COL_OID];
  auto *const next_col_oid_ptr = insert_pr->AccessForceNotNull(next_col_oid_offset);
  *(reinterpret_cast<uint32_t *>(next_col_oid_ptr)) = static_cast<uint32_t>(next_col_oid);

  // Write the schema_ptr into the PR
  const auto schema_ptr_offset = pr_map[REL_SCHEMA_COL_OID];
  auto *const schema_ptr_ptr = insert_pr->AccessForceNotNull(schema_ptr_offset);
  *(reinterpret_cast<uintptr_t *>(schema_ptr_ptr)) = reinterpret_cast<uintptr_t>(schema_ptr_ptr);

  // Set table_ptr to NULL because it gets set by execution layer after instantiation
  const auto table_ptr_offset = pr_map[REL_PTR_COL_OID];
  insert_pr->SetNull(table_ptr_offset);

  // Write the kind into the PR
  const auto kind_offset = pr_map[RELKIND_COL_OID];
  auto *const kind_ptr = insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<char *>(kind_ptr)) = static_cast<char>(postgres::ClassKind::REGULAR_TABLE);

  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>(name.data()), name.size());
  }

  // Write the name into the PR
  const auto name_offset = pr_map[RELNAME_COL_OID];
  auto *const name_ptr = insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Insert into pg_class table
  const auto tuple_slot = classes_->Insert(txn, insert_redo);

  // Get PR initializers and allocate a buffer from the largest one
  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const index_buffer = common::AllocationUtil::AllocateAligned(name_index_init.ProjectedRowSize());

  // Insert into oid_index
  auto *index_pr = oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<uint32_t *>(index_pr->AccessForceNotNull(0))) = static_cast<uint32_t>(table_oid);
  if (!classes_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into name_index
  index_pr = name_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;
  if (!classes_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into namespace_index
  index_pr = ns_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<uint32_t *>(index_pr->AccessForceNotNull(0))) = static_cast<uint32_t>(ns_oid);
  const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, tuple_slot);
  TERRIER_ASSERT(!result, "Insertion into non-unique namespace index failed.");

  delete[] index_buffer;

  return true;
}

}  // namespace terrier::catalog
