view: dt_order_switching {
  view_label: "Column group and Occurrence"
  derived_table: {
    sql:
        with orders_segmented as (
           select
              order_id,
              customer_id,
              processed_at,
              customer_order_number,
              customer_order_number = 1 as is_first_order,
              coalesce(discount_code = 'BLACKFRIDAY',false) is_black_friday_order,
              coalesce(customer_order_number = min(case when is_black_friday_order then customer_order_number end) over (partition by customer_id) -1,false) as is_last_order_before_bfcm,
              coalesce(customer_order_number = max(case when is_black_friday_order then customer_order_number end) over (partition by customer_id) + 1,false) as is_next_order_after_bfcm,
              coalesce(customer_order_number < min(case when is_black_friday_order then customer_order_number end) over (partition by customer_id),false) as is_orders_before_bfcm

           from
           -- if prod -- analytics.analytics.fct_orders
           -- if dev -- analytics.{{_user_attributes['dbt_schema']}}.fct_orders
           where is_valid_ecomm_order
              and {% condition affinity_timeframe %} processed_at {% endcondition %}
              and {% condition customer_order_category %} customer_order_category {% endcondition %}
        ),

        order_items_joined as (
            select
                order_items.customer_id,
                order_items.order_id,
                orders_segmented.processed_at,
                orders_segmented.is_first_order,
                order_items.category,
                order_items.sub_category,
                order_items.item_title,
                orders_segmented.customer_order_number,
                orders_segmented.is_black_friday_order,
                orders_segmented.is_last_order_before_bfcm,
                orders_segmented.is_next_order_after_bfcm,
                orders_segmented.is_orders_before_bfcm

            from
           -- if prod -- analytics.analytics.fct_order_items
           -- if dev -- analytics.{{_user_attributes['dbt_schema']}}.fct_order_items
            as order_items
            inner join orders_segmented
                on order_items.order_id = orders_segmented.order_id

        ),

        customers_agg as (
            select
                customer_id,
                listagg(distinct category,',') as categories
            from order_items_joined
            where {% parameter use_case %}
            group by 1
            having {% condition purchase_category %} categories {% endcondition %}
        ),

        order_items_filtered as (
            select * from order_items_joined
            where
                {% if use_case._parameter_value == "true" %}
                  1=1
                {% else %}
                  exists (
                      select 1 from customers_agg
                      where customers_agg.customer_id = order_items_joined.customer_id
                  )
                {% endif %}
        ),

        first_group as (
            select * from order_items_filtered
            where
                {% if use_case._parameter_value == "is_orders_before_bfcm" %}
                  is_last_order_before_bfcm
                {% elsif use_case._parameter_value == "is_first_order" %}
                  customer_order_number = 1
                {% else %}
                  1=1
                {% endif %}
              and category is not null
        ),

        second_group as (
            select * from order_items_filtered
            where
                {% if use_case._parameter_value == "is_orders_before_bfcm" %}
                  is_black_friday_order
                {% elsif use_case._parameter_value == "is_first_order" %}
                  customer_order_number = 2
                {% else %}
                  1=1
                {% endif %}
              and category is not null
        ),

        third_group as (
            select * from order_items_filtered
            where
                {% if use_case._parameter_value == "is_orders_before_bfcm" %}
                  is_next_order_after_bfcm
                {% elsif use_case._parameter_value == "is_first_order" %}
                  customer_order_number = 3
                {% else %}
                  1=1
                {% endif %}
              and category is not null
        )

        select
            first_group.{% parameter product_level %} as first_group,
            coalesce(second_group.{% parameter product_level %}, 'No purchase') as second_group,
            coalesce(third_group.{% parameter product_level %}, 'No purchase') as third_group,
            count(*) as number_of_occurrence

        from first_group
        left join second_group
            on first_group.customer_id = second_group.customer_id
        left join third_group
            on second_group.customer_id = third_group.customer_id
        where second_group.customer_id is not null

        group by 1,2,3



    ;;
  }

  filter: affinity_timeframe {
    view_label: "Filter Group"
    description: "Filter orders' process time"
    type: date
  }

  filter: customer_order_category {
    view_label: "Filter Group"
    description: "Indicator for first time/returning and subscription customers"
    type: string
    suggest_explore: fct_orders
    suggest_dimension: fct_orders_segmented.customer_order_category
  }

  filter: purchase_category {
    view_label: "Filter Group"
    description: "filter the distinct the only category customer purchase"
    type: string
    suggest_explore: fct_orders
    suggest_dimension: fct_order_items.category

  }

  parameter: use_case {
    view_label: "Filter Group"
    description: "Use case to apply Order switching"
    type: unquoted
    default_value: "true"
    allowed_value: {
      label: "Customers who only order only 1 category before the BFCM order"
      value: "is_orders_before_bfcm"
    }
    allowed_value: {
      label: "Customers' first order with only 1 category"
      value: "is_first_order"
    }
    allowed_value: {
      label: "None"
      value: "true"
    }
  }

  parameter: product_level {
    view_label: "Filter Group"
    description: "parameter to control display of product level"
    type: unquoted
    default_value: "category"
    allowed_value: {
      label: "Product Title"
      value: "item_title"
    }
    allowed_value: {
      label: "Product Category"
      value: "category"
    }
    allowed_value: {
      label: "Product Sub Category"
      value: "sub_category"
    }
  }

  dimension: pk {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${first_group} || ${second_group} || ${third_group};;
  }

  dimension: first_group {
    label: "
    {% if use_case._parameter_value == 'is_orders_before_bfcm' %}
        Last @{metric_labels} before BFCM orders
    {% elsif use_case._parameter_value == 'is_first_order' %}
        @{metric_labels} from First Orders  With 1 Category
    {% else %}
        @{metric_labels} 1
    {% endif %}
    "
    description: "Product Level of first order group. The use case and display name is controlled by filter 'Use Case'"
    type: string
    sql: ${TABLE}.first_group ;;

  }

  dimension: second_group {
    label: "
    {% if use_case._parameter_value == 'is_orders_before_bfcm' %}
      BFCM @{metric_labels}
    {% elsif use_case._parameter_value == 'is_first_order' %}
      @{metric_labels} From Second Order
    {% else %}
      @{metric_labels} 2
    {% endif %}
    "
    description: "Product Level of second order group. The use case and display name is controlled by filter 'Use Case'"
    type: string
    sql: ${TABLE}.second_group ;;

  }

  dimension: third_group {
    label: "
    {% if use_case._parameter_value == 'is_orders_before_bfcm' %}
      Next @{metric_labels} after BFCM orders
    {% elsif use_case._parameter_value == 'is_first_order' %}
      @{metric_labels} From Third Order
    {% else %}
      @{metric_labels} 3
    {% endif %}
    "
    description: "Product Level of third order group. The use case and display name is controlled by filter 'Use Case'"
    type: string
    sql: ${TABLE}.third_group ;;

  }

  dimension: number_of_occurrence {
    hidden: yes
    type: string
    sql: ${TABLE}.number_of_occurrence ;;

  }

  measure: total_occurrence {
    description: "How many times each combination of groups occurs"
    type: sum
    sql: ${number_of_occurrence} ;;
  }


}
