--
-- Copyright 2016 Roland Bouman, Just-Bi.nl
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
drop PROCEDURE p_basecol_view_usage;
--
-- PROCEDURE p_basecol_view_usage
-- 
-- Lists base columns on which a analytic, attribute or calculation views depend.
--
-- This procedure works by using the schema_name, table_name 
-- to select analytic, attribute, and calculation views from OBJECT_DEPENDENCIES.
-- 
-- (If the recursive flag is non-zero, then it will also look for indirect dependenies)
--
-- The dependencies found inOBJECT_DEPENDENCIES are used to lookup the model of the views
-- from the cdata column of _SYS_REPO.ACTIVE_OBJECT.
-- This contains a xml document (as nlcob) which contains the view definition.
-- This is parsed using p_parse_xml, after which the resulting table of DOM nodes is queried.
--
-- The XML parse tree is then examined to look for column usages 
-- 
create procedure p_basecol_view_usage(
  -- schema name LIKE pattern to look for.
  p_schema_name     nvarchar(128)
  -- table name LIKE pattern to look for.
, p_table_name      nvarchar(128)  default '%'
  -- column name LIKE pattern to look for.
, p_column_name     nvarchar(128)  default '%'
  -- package name LIKE pattern to look for.
, p_package_id      nvarchar(128)  default '%'
  -- view name LIKE pattern to look for.
, p_object_name     nvarchar(128)  default '%'
  -- view name LIKE pattern to look for.
, p_object_suffix   nvarchar(128)  default '%'
  -- whether to examine only direct (0) or also indirect (1) dependencies.
, p_recursive       tinyint        default 1
  -- result table: base columns on which the specified view(s) depends.
, out p_cols table (
  -- schema name of the referenced base column
    schema_name nvarchar(128)
  -- table name of the referenced base column
  , table_name  nvarchar(128)
  -- column name of the referenced base column
  , column_name nvarchar(128)
  -- list of view names that depend on the base column
  , views       nvarchar(5000)
  )
)
language sqlscript
sql security invoker
as
begin
  declare tab_cols table (
  -- schema name of the referenced base column
    schema_name nvarchar(128)
  -- table name of the referenced base column
  , table_name  nvarchar(128)
  -- column name of the referenced base column
  , column_name nvarchar(128)
  -- list of view names that depend on the base column
  , views       nvarchar(5000)
  );
  
  declare cursor c_views for 
    with params as (
      select    :p_schema_name p_schema_name 
      ,         :p_table_name  p_table_name
      ,         :p_column_name p_column_name
      ,         :p_package_id  p_package_id
      ,         :p_object_name p_object_name
      from      dummy
      )
    select      distinct 
                substr_before(dependent_object_name, '/') package_id
    ,           substr_after(dependent_object_name, '/')  object_name
    from        params                     p
    inner join  object_dependencies        od
    on          od.base_schema_name like p.p_schema_name
    and         od.base_object_name like p.p_table_name
    and         od.dependency_type  = 1 -- I think we can safely keep this at 1 (not 2)
    and         od.base_object_type      = 'TABLE'
    and         od.dependent_schema_name = '_SYS_BIC'
    and         od.dependent_object_type = 'VIEW'
    and         substr_before(dependent_object_name, '/') like p.p_package_id
    and         substr_after(dependent_object_name, '/')  like p.p_object_name
  ;
  
  -- suppress warning about selecting from unassigned table variable.
  p_cols = select '' schema_name
           ,      '' table_name
           ,      '' column_name
           ,      '' views
           from   dummy
           where  1 = 0;
  
  for r_view as c_views do
    call p_get_view_basecols (
      r_view.package_id
    , r_view.object_name
    , :p_object_suffix
    , p_recursive
    , tab_cols
    );
    
    p_cols  =
      select   schema_name
      ,        table_name
      ,        column_name
      ,        string_agg(views, ', ') views
      from    (
        select  *
        from    :tab_cols  tc
        where   tc.schema_name like p_schema_name
        and     tc.table_name  like p_table_name
        and     tc.column_name like p_column_name
        union 
        select  *
        from    :p_cols pc
      )
      group by  schema_name
      ,         table_name
      ,         column_name
    ;
  end for;
  
end;
