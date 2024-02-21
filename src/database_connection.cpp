#include <iostream>
#include <memory>
#include <pqxx/pqxx>
#include <sstream>
#include <type_traits>
#include <utility>
#include <variant>

#include "database_connection.hpp"
#include "helpers.hpp"

namespace kdb
{
bool db_cxn::set_config(dbconfig config)
{
  m_config  = config;
  m_db_name = config.credentials.name;
  return true;
}

pqxx::result db_cxn::do_insert(DatabaseQuery query)
{
  pqxx::connection connection(connection_string().c_str());
  pqxx::work worker(connection);
  pqxx::result pqxx_result = worker.exec(insert_statement(query));
  worker.commit();

  return pqxx_result;
}

pqxx::result db_cxn::do_insert(InsertReturnQuery query, std::string returning)
{
  std::string table = query.table;
  pqxx::connection connection(connection_string().c_str());
  pqxx::work worker(connection);
  pqxx::result pqxx_result = worker.exec(insert_statement(query, returning));
  worker.commit();

  return pqxx_result;
}

pqxx::result db_cxn::do_update(UpdateReturnQuery query, std::string returning)
{
  std::string table = query.table;
  pqxx::connection connection(connection_string().c_str());
  pqxx::work worker(connection);
  pqxx::result pqxx_result = worker.exec(update_statement(query, returning));
  worker.commit();

  return pqxx_result;
}

template <typename T>
pqxx::result db_cxn::do_select(T query)
{
  pqxx::connection connection(connection_string().c_str());
  pqxx::work worker(connection);
  pqxx::result pqxx_result = worker.exec(select_statement(query));
  worker.commit();

  return pqxx_result;
}

template <typename T>
pqxx::result db_cxn::do_delete(T query)
{
  pqxx::connection connection(connection_string().c_str());
  pqxx::work worker(connection);
  pqxx::result pqxx_result = worker.exec(delete_statement(query));
  worker.commit();

  return pqxx_result;
}

std::string db_cxn::connection_string()
{
  std::string s{};
  s += "dbname = ";
  s += m_config.credentials.name;
  s += " user = ";
  s += m_config.credentials.user;
  s += " password = ";
  s += m_config.credentials.password;
  s += " hostaddr = ";
  s += m_config.address;
  s += " port = ";
  s += m_config.port;
  return s;
}

QueryResult db_cxn::query(DatabaseQuery query)
{
  switch (query.type)
  {
    case QueryType::INSERT:
    {
      try
      {
        pqxx::result pqxx_result = do_insert(query);
        return QueryResult{};
      }
      catch (const pqxx::sql_error &e)
      {
        std::cerr << e.what() << "\n" << e.query() << std::endl;
        throw e;
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << std::endl;
        throw e;
      }
    }
    case QueryType::SELECT:
    {
      pqxx::result pqxx_result = do_select(query);
      QueryResult result{.table = query.table};

      for (const auto &row : pqxx_result)
      {
        ResultMap values;
        int index = 0;
        for (const auto &value : row)
          values[query.fields[index++]] = value.c_str();
        result.values.push_back(values);

      }
      return result;
    }

    case QueryType::DELETE:
    {
      pqxx::result pqxx_result   = do_delete(query);
      QueryResult  result{.table = query.table};
      result.values.push_back(ResultMap{});
      for (const auto &row : pqxx_result)
        for (const auto &value : row)
          result.values.front()[query.filter.value().front().first] = value.c_str();
      return result;
    }

    case QueryType::UPDATE:
      return QueryResult{};

  }
  return QueryResult{};
}

template <typename T>
QueryResult db_cxn::query(T query)
{
  pqxx::result pqxx_result = do_select(query);
  QueryResult result{.table = query.table};

  for (const auto &row : pqxx_result)
  {
    int index{};
    ResultMap values;
    for (const auto &value : row)
      values[query.fields[index++]] = value.c_str();
    result.values.push_back(values);
  }
  return result;
}

template QueryResult db_cxn::query(
  MultiVariantFilterSelect<std::vector<std::variant<CompFilter, CompBetweenFilter>>>);

template QueryResult db_cxn::query(
  MultiVariantFilterSelect<std::vector<std::variant<CompFilter, CompBetweenFilter, MultiOptionFilter>>>);

template QueryResult db_cxn::query(
  MultiVariantFilterSelect<std::vector<std::variant<CompBetweenFilter, QueryFilter>>>);

template QueryResult db_cxn::query(
  MultiVariantFilterSelect<std::vector<std::variant<QueryComparisonFilter, QueryFilter>>>);

template QueryResult db_cxn::query(
  JoinQuery<std::vector<std::variant<CompFilter, CompBetweenFilter>>>);

template QueryResult db_cxn::query(
  JoinQuery<std::vector<QueryFilter>>);

template QueryResult db_cxn::query(
  JoinQuery<QueryFilter>);

template QueryResult db_cxn::query(
  SimpleJoinQuery);

template QueryResult db_cxn::query(
  JoinQuery<std::vector<std::variant<CompFilter, CompBetweenFilter, MultiOptionFilter>>>);

template QueryResult db_cxn::query<ComparisonSelectQuery>(ComparisonSelectQuery);

std::string db_cxn::query(InsertReturnQuery query)
{
  if (const pqxx::result pqxx_result = do_insert(query, query.returning); !pqxx_result.empty())
  {
    const auto row = pqxx_result.at(0);
    if (!row.empty())
      return row.at(0).as<std::string>();
  }
  return "";
}

std::string db_cxn::query(UpdateReturnQuery query)
{
  if (const pqxx::result pqxx_result = do_update(query, query.returning); !pqxx_result.empty())
  {
    const auto row = pqxx_result.at(0);
    if (!row.empty())
      return row.at(0).as<std::string>();
  }
  return "";
}

std::string db_cxn::name() { return m_db_name; }
} // ns kdb
