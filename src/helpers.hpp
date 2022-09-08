#include <db_structs.hpp>


//  ┌──────────────────────────────────────────┐  //
//  │░░░░░░░░░░░░░░ Helper utils ░░░░░░░░░░░░░░░│  //
//  └──────────────────────────────────────────┘  //

std::string fieldsAsString(std::vector<std::string> fields)
{
  std::string field_string = "";
  std::string delim        = "";

  for (const auto &field : fields)
  {
    field_string += delim + field;
    delim         = ",";
  }

  return field_string;
}
//**************************************************//
std::string valuesAsString(StringVec values, size_t number_of_fields)
{
  std::string value_string{"VALUES ("};
  std::string delim{};
  int         index{1};

  for (const auto &value : values)
  {
    delim = (index++ % number_of_fields == 0) ? "),(" : ",";
    value_string += "'";
    value_string += (value.empty()) ? "NULL" : DoubleSingleQuotes(value);
    value_string += "'" + delim;
  }
  value_string.erase(value_string.end() - 2, value_string.end());

  return value_string;
}
//**************************************************//
static std::string orderStatement(const OrderFilter& filter)
{
  return " ORDER BY " + filter.field + ' ' + filter.order;
}
//**************************************************//
static std::string limitStatement(const std::string& number)
{
  return " LIMIT " + number;
}

//  ┌─────────────────────────────────────┐  //
//  │░░░░░░░░░░░░░░ Visitors ░░░░░░░░░░░░░░│  //
//  └─────────────────────────────────────┘  //
template <typename T>
struct FilterVisitor
{
FilterVisitor(T filters)
{
  operator()(filters);
}

std::string
value() const
{
  return f_s;
}
//**************************************************//
void
operator()(const MultiOptionFilter& f)
{
  f_s += f.a + " " + f.comparison + " (";
  std::string delim{};
  for (const auto &option : f.options)
  {
    f_s += delim + option;
    delim = ",";
  }
  f_s += ")";
}
//**************************************************//
void
operator()(const CompBetweenFilter& filter)
{
  f_s += filter.field + " BETWEEN " + filter.a + " AND " + filter.b;
}
//**************************************************//
void
operator()(const CompFilter& filter)
{
  f_s += filter.a + filter.sign + filter.b;
}
//**************************************************//
void
operator()(const QueryComparisonFilter& filter)
{
  f_s += std::get<0>(filter[0]) + std::get<1>(filter[0]) + std::get<2>(filter[0]);
}
//**************************************************//
void
operator()(const QueryFilter& filter)
{
  std::string delim{};
  for (const auto& f : filter)
  {
    f_s += delim + f.first + '=' + '\'' + f.second + '\'';
    delim = " AND ";
  }
}
//**************************************************//
void
operator()(QueryFilter::Filters filters)
{
  std::string delim{};
  for (const auto& f : filters)
  {
    f_s += delim + f.first + '=' + '\'' + f.second + '\'';
    delim = " AND ";
  }
}
//**************************************************//
void
operator()(GenericFilter filter)
{
  f_s = filter.a + filter.comparison + filter.b;
}

std::string f_s;
};

//  ┌─────────────────────────────────────┐  //
//  │░░░░░░░░░░ Visitor Helpers ░░░░░░░░░░░│  //
//  └─────────────────────────────────────┘  //
template <typename T>
std::string filterStatement(T filter)
{
  return FilterVisitor{filter}.value();
}
//**************************************************//
template <typename FilterA, typename FilterB>
std::string getVariantFilterStatement(
    std::vector<std::variant<FilterA, FilterB>> filters)
{
  std::string filter_string{};
  uint8_t     idx          = 0;
  uint8_t     filter_count = filters.size();

  for (const auto &filter : filters)
  {
    filter_string += (filter.index() == 0) ? filterStatement(std::get<0>(filter)) :
                                             filterStatement(std::get<1>(filter));
    if (filter_count > (idx + 1))
    {
      idx++;
      filter_string += " AND ";
    }
  }

  return filter_string;
}
//**************************************************//
template <typename FilterA, typename FilterB, typename FilterC>
std::string getVariantFilterStatement(std::vector<std::variant<FilterA, FilterB, FilterC>> filters)
{
  std::string filter_string{};
  uint8_t     idx          = 0;
  uint8_t     filter_count = filters.size();

  for (const auto &filter : filters)
  {
    switch (filter.index())
    {
    case (0): filter_string += filterStatement(std::get<0>(filter));
    break;
    case (1): filter_string += filterStatement(std::get<1>(filter));
      break;
    default:  filter_string += filterStatement(std::get<2>(filter));
    }

    if (filter_count > (idx + 1))
    {
      idx++;
      filter_string += " AND ";
    }
  }
  return filter_string;
}

//******************************************************************************************//

static const char* UNSUPPORTED = "SELECT 1";
template <typename T>
struct SelectVisitor
{
std::string delim         = "";
std::string filter_string = " WHERE ";
std::string query;

SelectVisitor(T query)
{
  const auto filter = query.filter;
  if (filter.size())
    operator()(filter);
  else
  {
    if constexpr(std::is_same_v<T, JoinQuery<QueryFilter>>)
      query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + ' ' + getJoinStatement(query.joins);
    else
    if constexpr(std::is_same_v<T, SimpleJoinQuery>)
      query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + ' ' + getJoinStatement({query.join});
    else
      query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table;
  }
}
//**************************************************//
std::string
value() const
{
  return query;
}
//**************************************************//
void
operator()(Query filter)
{
  if (filter.size() > 1 && filter.front().first == filter.at(1).first)
  {
    filter_string += filter.front().first + " in (";
    for (const auto &filter_pair : filter)
    {
      filter_string += delim + filter_pair.second;
      delim = ",";
    }
    query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string + ")";
  }
  else
  {
    for (const auto &filter_pair : filter)
      filter_string += delim + filter_pair.first + "='" + filter_pair.second + "'"; delim = " AND ";

    query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string;
  }
}
//**************************************************//
void
operator()(DatabaseQuery filter)
{
  if (filter.size() > 1 &&
      filter.front().first == filter.at(1).first)
  {
    filter_string += filter.front().first + " in (";
    for (const auto &filter_pair : filter)
    {
      filter_string += delim + filter_pair.second;
      delim = ",";
    }
    query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string + ")";
  }
  else
  {
    for (const auto &filter_pair : filter)
    {
      filter_string += delim + filter_pair.first + "='" + filter_pair.second + "'";
      delim = " AND ";
    }
    query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string;
  }
}
//**************************************************//
void
operator()(ComparisonSelectQuery filter)
{
  if (filter.size() > 1)
  {
    query = UNSUPPORTED;
    return;
  }

  for (const auto &filter_tup : filter)
  {
    filter_string += delim + std::get<0>(filter_tup) + std::get<1>(filter_tup) + std::get<2>(filter_tup);
    delim = " AND ";
  }
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string;
}
//**************************************************//
void
operator()(ComparisonBetweenSelectQuery filter)
{
  if (filter.size() > 1)
  {
    query = UNSUPPORTED;
    return;
  }

  for (const auto &filter : filter)
    filter_string += delim + filterStatement(filter);
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string;
}
//**************************************************//
void
operator()(MultiFilterSelect filter)
{
  for (const auto &filter : filter)
  {
    filter_string += delim + filterStatement(filter);
    delim = " AND ";
  }
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string;
}
//**************************************************//
void
operator()(MultiVariantFilterSelect<std::vector<std::variant<CompFilter, CompBetweenFilter>>> filter)
{
  std::string stmt{"SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string +
    getVariantFilterStatement<CompFilter, CompBetweenFilter>(filter)};
  if (query.order.has_value()) stmt += orderStatement(query.order);
  if (query.limit.has_value()) stmt += limitStatement(query.limit.count);
  query = stmt;
}
//**************************************************//
void
operator()(MultiVariantFilterSelect<std::vector<std::variant<CompFilter, CompBetweenFilter, MultiOptionFilter>>> filter)
{
  std::string stmt{"SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string +
    getVariantFilterStatement<CompFilter, CompBetweenFilter, MultiOptionFilter>(filter)};
  if (query.order.has_value()) stmt += orderStatement(query.order);
  if (query.limit.has_value()) stmt += limitStatement(query.limit.count);
  query = stmt;
}
//**************************************************//
void
operator()(MultiVariantFilterSelect<std::vector<std::variant<CompBetweenFilter, QueryFilter>>> filter)
{
  std::string stmt{"SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string +
                    getVariantFilterStatement<CompBetweenFilter, QueryFilter>(filter)};
  if (query.order.has_value()) stmt += orderStatement(query.order);
  if (query.limit.has_value()) stmt += limitStatement(query.limit.count);
  return stmt;
}
//**************************************************//
void
operator()(MultiVariantFilterSelect<std::vector<std::variant<QueryComparisonFilter, QueryFilter>>> filter)
{
  std::string stmt{"SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + filter_string +
                    getVariantFilterStatement<QueryComparisonFilter, QueryFilter>(filter)};
  if (query.order.has_value()) stmt += orderStatement(query.order);
  if (query.limit.has_value()) stmt += limitStatement(query.limit.count);
  query = stmt;
}
//**************************************************//
void
operator()(JoinQuery<std::vector<std::variant<CompFilter, CompBetweenFilter, MultiOptionFilter>>> filter)
{
  filter_string += getVariantFilterStatement(filter);
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + " " + getJoinStatement(query.joins) + filter_string;
}
//**************************************************//
void
operator()(SimpleJoinQuery filter)
{
  filter_string += filterStatement(filter);
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + " " + getJoinStatement({query.join}) + filter_string;
}
//**************************************************//
void
operator()(JoinQuery<std::vector<QueryFilter>> filter)
{
  for (const auto &f : filter)
  {
    filter_string += delim + filterStatement(f);
    delim = " AND ";
  }
  std::string join_string = getJoinStatement(query.joins);
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + " " + join_string + filter_string;
}
//**************************************************//
void
operator()(JoinQuery<QueryFilter>)
{
  filter_string          += delim + filterStatement(filter);
  std::string join_string = getJoinStatement(query.joins);
  query = "SELECT " + fieldsAsString(query.fields) + " FROM " + query.table + " " + join_string + filter_string;
}

};

template <typename T>
std::string selectStatement(T query)
{
  return SelectVisitor{query}.value();
}
