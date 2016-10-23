/**
* Copyright 2016 Roland Bouman, Just-Bi.nl
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
drop PROCEDURE p_get_view_basecols;
/**
* PROCEDURE p_get_view_basecols
* 
* Lists base columns (database table/view columns) on which a analytical, attribute or calculation views depend.
*
* This procedure works by using the package_id, object_name and object_suffix (passed as arguments) 
* to select analytical, attribute, and calculation views from _SYS_REPO.ACTIVE_OBJECT.
* 
* (If the recursive flag is non-zero, then the list of selected items is expanded using a query on OBJECT_DEPENDENCIES,
* to look again in _SYS_REPO.ACTIVE_OBJECT for any analytical, attribute and calculation views used by the selected objects.)
*
* From _SYS_REPO.ACTIVE_OBJECT the contents of the cdata column are examined. 
* This contains a xml document (as nlcob) which contains the view definition.
* This is parsed using p_parse_xml, after which the resulting table of DOM nodes is queried.
*
* This procedure only determines whether the base column is referenced by the views.
* If a base column is referenced, but not used in any way later on in the dataflow of the view, then it will still be reproted.
* 
*/
create PROCEDURE p_get_view_basecols (
  -- package name pattern. Used to match packages containing analytical, attribute or calculation views. Can contain LIKE wildcards.
  p_package_id    nvarchar(255)
  -- object name pattern. Used to match name of analytical, attribute or calculation views. Can contain LIKE wildcards.
, p_object_name   nvarchar(255) default '%'
  -- object suffix pattern. Can be used to specify the type of view. Can contain LIKE wildcards.
, p_object_suffix nvarchar(255) default '%'
  -- flag to indicate whether to recursively analyze analytical, attribute or calculation views on which the view to be analyzed depends. 
  -- 0 means only look at the given view, 1 means also look at underlying views.
, p_recursive     tinyint default 1
  -- result table: base columns on which the specified view(s) depends.
, out p_cols table (
  -- schema name of the referenced base column
    schema_name nvarchar(128)
  -- table name of the referenced base column
  , table_name  nvarchar(128)
  -- column name of the referenced base column
  , column_name nvarchar(128)
  -- list of view names that depend on the base column
  , views       nclob
  )
)
language sqlscript
sql security invoker
as
begin
  declare v_row_num integer default 0;
  declare v_package_ids nvarchar(255) array;
  declare v_object_names nvarchar(255) array;
  declare v_schema_names nvarchar(128) array;
  declare v_table_names nvarchar(128) array;
  declare v_column_names nvarchar(128) array;
  declare v_object_suffix nvarchar(255);

  declare tab_dom table(
    node_id           int           
  , parent_node_id    int           
  , node_type         tinyint       
  , node_name         nvarchar(64)  
  , node_value        nclob         
  , token_text        nclob         
  , pos               int           
  , len               int           
  );
  
  declare cols table (
    package_id nvarchar(255)
  , object_name nvarchar(255)
  , schema_name nvarchar(128)
  , table_name nvarchar(128)
  , column_name nvarchar(128)
  );
  
  declare cursor c_views for
    with top_level_views as (
      select  package_id
      ,       object_name
      ,       object_suffix
      from    "_SYS_REPO"."ACTIVE_OBJECT"
      where   package_id like :p_package_id
      and     object_name like :p_object_name
      and     object_suffix like :p_object_suffix
      and     object_suffix in (
                'analyticalview'
              , 'attributeview'
              , 'calculationview'
              )
      )
    , top_level_and_base_views as (
      select     tv.package_id
      ,          tv.object_name
      ,          tv.object_suffix
      from       top_level_views tv
      union
      select     ao.package_id
      ,          ao.object_name
      ,          ao.object_suffix
      from       top_level_views     tv
      inner join object_dependencies od
      on         tv.package_id||'/'||tv.object_name = od.dependent_object_name
      and        '_SYS_BIC'                         = od.dependent_schema_name
      and        'VIEW'                             = od.dependent_object_type
      and        '_SYS_BIC'                         = od.base_schema_name
      and        'VIEW'                             = od.base_object_type
      inner join _SYS_REPO.ACTIVE_OBJECT ao
      on          substr_before(od.base_object_name, '/') = ao.package_id
      and         substr_after(od.base_object_name, '/') = ao.object_name
      and         ao.object_suffix in (
                    'analyticalview'
                  , 'attributeview'
                  , 'calculationview'
                  )
      where       :p_recursive = 1
      )
      select      v.package_id
      ,           v.object_name
      ,           v.object_suffix
      ,           v.cdata
      from        top_level_and_base_views tbv
      inner join  _SYS_REPO.ACTIVE_OBJECT  v
      on          tbv.package_id    = v.package_id
      and         tbv.object_name   = v.object_name
      and         tbv.object_suffix = v.object_suffix    
    ;

  for r_view as c_views do
    v_object_suffix = r_view.object_suffix;
    call p_parse_xml(r_view.cdata, tab_dom);
    begin
      declare cursor c_deps for 
        select     keyMapping_schemaName.node_value         schema_name
        ,          keyMapping_columnObjectName.node_value   table_name
        ,          keyMapping_columnName.node_value         column_name
        from       :tab_dom keyMapping
        inner join :tab_dom keyMapping_schemaName
        on         keyMapping.node_type = 1
        and        keyMapping.node_name = 'keyMapping'
        and        keyMapping.node_id = keyMapping_schemaName.parent_node_id
        and        2 = keyMapping_schemaName.node_type
        and        'schemaName' = keyMapping_schemaName.node_name
        inner join :tab_dom keyMapping_columnObjectName
        on         keyMapping.node_id = keyMapping_columnObjectName.parent_node_id
        and        2 = keyMapping_columnObjectName.node_type
        and        'columnObjectName' = keyMapping_columnObjectName.node_name
        inner join :tab_dom keyMapping_columnName
        on         keyMapping.node_id = keyMapping_columnName.parent_node_id
        and        2 = keyMapping_columnName.node_type
        and        'columnName' = keyMapping_columnName.node_name
        where      :v_object_suffix in (
                     'analyticalview'
                   , 'attributeview'
                   )
        union all
        select     ds_co_schemaName.node_value              schema_name
        ,          ds_co_columnObjectName.node_value        table_name
        ,          cv_input_mapping_source.node_value       column_name
        from       :tab_dom ds
        inner join :tab_dom ds_type
        on         ds.node_type = 1
        and        ds.node_name = 'DataSource'
        and        ds.node_id = ds_type.parent_node_id
        and        2 = ds_type.node_type
        and        'type' = ds_type.node_name
        and        'DATA_BASE_TABLE' = cast(ds_type.node_value as varchar(128))
        inner join :tab_dom ds_id
        on         ds.node_id = ds_id.parent_node_id
        and        2 = ds_id.node_type
        and        'id' = ds_id.node_name
        inner join :tab_dom ds_co
        on         ds.node_id = ds_co.parent_node_id
        and        1 = ds_co.node_type
        and        'columnObject' = ds_co.node_name
        inner join :tab_dom ds_co_schemaName
        on         ds_co.node_id = ds_co_schemaName.parent_node_id
        and        2 = ds_co_schemaName.node_type
        and        'schemaName' = ds_co_schemaName.node_name
        inner join :tab_dom ds_co_columnObjectName
        on         ds_co.node_id = ds_co_columnObjectName.parent_node_id
        and        2 = ds_co_columnObjectName.node_type
        and        'columnObjectName' = ds_co_columnObjectName.node_name
        inner join :tab_dom cv_input_node
        on         'node' = cv_input_node.node_name
        and        2 = cv_input_node.node_type
        and        '#'||ds_id.node_value = cast(cv_input_node.node_value as nvarchar(128))
        inner join :tab_dom cv_input_mapping
        on         'mapping' = cv_input_mapping.node_name
        and        1 = cv_input_mapping.node_type
        and        cv_input_node.parent_node_id = cv_input_mapping.parent_node_id
        inner join :tab_dom cv_input_mapping_source
        on         'source' = cv_input_mapping_source.node_name
        and        2 = cv_input_mapping_source.node_type
        and        cv_input_mapping.node_id = cv_input_mapping_source.parent_node_id
        where      :v_object_suffix = 'calculationview'
      ;
      for r_deps as c_deps do
        v_row_num = v_row_num + 1;
      
        v_package_ids[v_row_num] = r_view.package_id;
        v_object_names[v_row_num] = r_view.object_name;
        v_schema_names[v_row_num] = r_deps.schema_name;
        v_table_names[v_row_num] = r_deps.table_name;
        v_column_names[v_row_num] = r_deps.column_name;
      end for;
    end; 
  end for;
  
  cols = unnest(
    :v_package_ids
  , :v_object_names
  , :v_schema_names
  , :v_table_names
  , :v_column_names
  ) as (
    package_id
  , object_name
  , schema_name
  , table_name
  , column_name
  );
  
  p_cols = select   
           schema_name
  ,        table_name
  ,        column_name
  ,        string_agg(package_id||'/'||object_name, ', ') views 
  from     (select distinct * 
            from :cols
            order by schema_name
            ,        table_name
            ,        column_name
            ,        package_id
            ,        object_name
           )
  group by schema_name
  ,        table_name
  ,        column_name
  order by schema_name
  ,        table_name
  ,        column_name  
  ;
end;
