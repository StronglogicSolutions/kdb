#pragma once

#include <variant>
#include <iostream>
#include "database_connection.hpp"
#include <memory>

namespace kdb {
class KDB {
 public:
  KDB(dbconfig config = {}) : m_connection(std::move(std::unique_ptr<db_cxn>{new db_cxn}))
  {
    if (!config.validate())
      throw std::invalid_argument{"Please provide valid kdb::dbconfig object"};
    m_connection->set_config(config);
  }

  KDB(KDB&& k) :
    m_connection(std::move(k.m_connection)),
    m_credentials(std::move(k.m_credentials))
  {}

  KDB(std::unique_ptr<db_cxn> db_connection, dbconfig config)
  : m_connection(std::move(db_connection))
  {
    m_connection->set_config(config);
  }

  ~KDB() = default;

QueryValues select(std::string table, Fields fields, QueryFilter filter = {}, uint32_t limit = 0) const
{
  try
  {
    QueryResult result = m_connection->query(
      DatabaseQuery{
        .table = table,
        .fields = fields,
        .type = QueryType::SELECT,
        .values = {},
        .filter = filter});
    return result.values;
  }
  catch (const pqxx::sql_error& e)
  {
    throw e;
  }
  catch (const std::exception& e)
  {
    throw e;
  }
}

QueryValues select(std::string table, Fields fields,
                    QueryComparisonFilter filter = {})
{
  try
  {
    QueryResult result = m_connection->query(
      ComparisonSelectQuery{
        .table = table,
        .fields = fields,
        .values = {},
        .filter = filter
      });
    return result.values;

  }
  catch (const pqxx::sql_error &e)
  {
    throw e;
  }
  catch (const std::exception &e)
  {
    throw e;
  }
}

  QueryValues selectCompare(std::string table, Fields fields,
                            std::vector<CompFilter> filter = {})
  {
    try
    {
      ComparisonBetweenSelectQuery select_query{
        .table  = table,
        .fields = fields,
        .values = {},
        .filter = filter
      };
      QueryResult result = m_connection->query(select_query);
      return result.values;

    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  QueryValues selectMultiFilter(std::string table, Fields fields,
                                std::vector<GenericFilter> filters,
                                const OrderFilter&         order = OrderFilter{},
                                const LimitFilter&         limit = LimitFilter{})
  {
    try
    {
      MultiFilterSelect select_query{
        .table  = table,
        .fields = fields,
        .filter = filters,
        .order  = order,
        .limit  = limit
      };
      QueryResult result = m_connection->query(select_query);
      return result.values;
    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  template <typename FilterA, typename FilterB>
  QueryValues selectMultiFilter(
      std::string table, Fields fields,
      std::vector<std::variant<FilterA, FilterB>> filters)
      {
    try
    {
      MultiVariantFilterSelect<std::vector<std::variant<FilterA, FilterB>>> select_query{
        .table  = table,
        .fields = fields,
        .filter = filters};
      QueryResult result = m_connection->query(select_query);
      return result.values;
    }
    catch (const pqxx::sql_error &e)
    {
      throw;
    }
    catch (const std::exception &e)
    {
      throw;
    }
  }

  template <typename FilterA, typename FilterB, typename FilterC>
  QueryValues selectMultiFilter(
      const std::string&                                          table,
      const Fields&                                               fields,
      const std::vector<std::variant<FilterA, FilterB, FilterC>>& filters,
      const OrderFilter&                                          order = OrderFilter{},
      const LimitFilter&                                          limit = LimitFilter{}
      )
  {
    try
    {
      MultiVariantFilterSelect<std::vector<std::variant<FilterA, FilterB, FilterC>>> select_query{
        .table  = table,
        .fields = fields,
        .filter = filters,
        .order  = order,
        .limit  = limit
      };
      QueryResult result = m_connection->query(select_query);
      return result.values;
    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  template <typename FilterA, typename FilterB>
  QueryValues selectMultiFilter(const std::string&                          table,
                                const Fields&                               fields,
                                std::vector<std::variant<FilterA, FilterB>> filters,
                                const OrderFilter&                          order,
                                const LimitFilter&                          limit)
  {
    try
    {
      MultiVariantFilterSelect<std::vector<std::variant<FilterA, FilterB>>> select_query{
        .table  = table,
        .fields = fields,
        .filter = filters,
        .order  = order,
        .limit  = limit
      };
      QueryResult result = m_connection->query(select_query);
      return result.values;
    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  template <typename T = std::vector<QueryFilter>>
  QueryValues selectJoin(const std::string& table,
                         const Fields&      fields,
                         const T&           filters,
                         const Joins&       joins,
                         const OrderFilter& order = OrderFilter{},
                         const LimitFilter& limit = LimitFilter{}) const
  {
    try
    {
      return m_connection->query(JoinQuery<T>{
        .table  = table,
        .fields = fields,
        .filter = filters,
        .joins  = joins,
        .order  = order,
        .limit  = limit}).values;
    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  QueryValues selectSimpleJoin(std::string table, Fields fields, QueryFilter filter, Join join)  const
  {
    try {
      SimpleJoinQuery select_query{
        .table  = table,
        .fields = fields,
        .filter = filter,
        .join   = join
      };
      QueryResult result = m_connection->query(select_query);
      return result.values;

    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

  std::string update(std::string table, Fields fields, Values values,
                     QueryFilter filter, std::string returning = "id")
  {
  try
  {
      UpdateReturnQuery update_query{
        .table     = table,
        .fields    = fields,
        .type      = QueryType::UPDATE,
        .values    = values,
        .filter    = filter,
        .returning = returning};

      return m_connection->query(update_query);
    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }

    return "";
  }

  std::string remove(std::string table, QueryFilter filter)
  {
    try
    {
      auto result = m_connection->query(
        DatabaseQuery{
          .table  = table,
          .fields = {},
          .type   = QueryType::DELETE,
          .values = {},
          .filter = filter});

      if (!result.values.empty())
        return result.values.at(0).second;

    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }

    return "";
  }

  bool insert(std::string table, Fields fields, Values values)
  {
    try
    {
      QueryResult result = m_connection->query(
        DatabaseQuery{
          .table  = table,
          .fields = fields,
          .type   = QueryType::INSERT,
          .values = values,
          .filter = QueryFilter{}});

    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }

    return true;
  }

  std::string insert(std::string table, Fields fields, Values values,
                     std::string returning)
  {
    try
    {
      return m_connection->query(
        InsertReturnQuery{
          .table     = table,
          .fields    = fields,
          .type      = QueryType::INSERT,
          .values    = values,
          .returning = returning});

    }
    catch (const pqxx::sql_error &e)
    {
      throw e;
    }
    catch (const std::exception &e)
    {
      throw e;
    }
  }

 private:
  std::unique_ptr<db_cxn> m_connection;
  identification          m_credentials;
};

}  // namespace kdb
