{%- macro android_interact_source_columns() -%}
{{ android_common_columns() }}

  , action

  , view
  , view_filter
  , {{ normalize_id("view_filter_id") }}      as view_filter_id
  , {{ normalize_id("view_id") }}             as view_id
  , {{ normalize_id("view_sort_id") }}        as view_sort_id

  , element
  , index                                     as element_index
  , {{ normalize_id("_id") }}                 as element_id

  , collection
  , {{ normalize_id("collection_id") }}       as collection_id
  , collection_index
  , component                                 as collection_type

{%- endmacro -%}


{%- macro apple_interact_source_columns() -%}
  {{ apple_common_columns() }}

  , action

  , view
  , null                                      as view_filter
  , {{ normalize_id("view_filter_id") }}      as view_filter_id
  , {{ normalize_id("view_type_id") }}        as view_id
  , null                                      as view_sort_id

  , null                                      as element
  , index                                     as element_index
  , {{ normalize_id("_id") }}                 as element_id

  , collection
  , {{ normalize_id("collection_id") }}       as collection_id
  , collection_index                          as collection_index
  , component                                 as collection_type

{%- endmacro -%}

{%- macro roku_interact_source_columns() -%}
  {{ roku_common_columns() }}

  , action

  , view
  , null                                      as view_filter
  , {{ normalize_id("view_filter_id") }}      as view_filter_id
  , {{ normalize_id("view_type_id") }}        as view_id
  , null                                      as view_sort_id

  , element
  , index                                     as element_index
  , {{ normalize_id("_id") }}                 as element_id

  , collection
  , {{ normalize_id("collection_id") }}       as collection_id
  , collection_index
  , component                                 as collection_type

{%- endmacro -%}

{%- macro web_interact_source_columns() -%}
  {{ web_common_columns() }}

  , UPPER(action)                             as action

  , view
  , null                                      as view_filter
  , null                                      as view_filter_id
  , null                                      as view_id
  , null                                      as view_sort_id

  , element
  , index                                     as element_index
  , {{ normalize_id("_id") }}                 as element_id

  , collection
  , {{ normalize_id("collection_id") }}       as collection_id
  , collection_index
  , component                                 as collection_type

{%- endmacro -%}

{%- macro interact_columns(additional_columns=[]) -%}
  {{ return(common_columns() + [
            "action"
            , "view"
            , "view_filter"
            , "view_filter_id"
            , "view_id"
            , "view_sort_id"
            , "element_index"
            , "element_id"

            , "collection_id"
            , "collection_index"
            , "collection_type"
      ] + additional_columns)
  }}
{%- endmacro -%}
