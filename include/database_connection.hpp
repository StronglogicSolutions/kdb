#pragma once

#include "db_structs.hpp"
#include "database_interface.hpp"
#include <pqxx/pqxx>

namespace kdb
{
class db_cxn : public DatabaseInterface {
public:
// constructor
  db_cxn() {}
  db_cxn(db_cxn&& d)
  : m_config(std::move(d.m_config)) {}
  db_cxn(const db_cxn& d) = delete;
  virtual ~db_cxn() final {}

  template <typename T>
  QueryResult         query(T query);
  std::string         query(InsertReturnQuery query);
  std::string         query(UpdateReturnQuery query);
  virtual QueryResult query(DatabaseQuery query) final;
  std::string         name();
  virtual bool        set_config(dbconfig config) final;

private:
  std::string         connection_string();
  pqxx::result        do_insert(DatabaseQuery query);
  pqxx::result        do_insert(InsertReturnQuery query, std::string returning);
  template <typename T>
  pqxx::result        do_select(T query);
  template <typename T>
  pqxx::result        do_delete(T query);
  pqxx::result        do_update(UpdateReturnQuery query, std::string returning);

  dbconfig     m_config;
  std::string  m_db_name;

};
} // ns kdb