# Power BI model

No `.pbix` is published and the Power BI service was not used. Connect Power BI to the dedicated-pool tables in `sql/synapse/dedicated/02_ctas_gold.sql` or, for a smaller model, to the serverless views in `sql/synapse/serverless/01_openrowset_gold.sql`.

The local Streamlit app in `dashboard/app.py` reads the same Gold Delta files and is labeled as local. It is not a substitute for the model below.

## Relationships

| From | To | Columns | Notes |
| --- | --- | --- | --- |
| fact_sales | dim_customer | customer_sk | As-of version, not only current |
| fact_sales | dim_product | product_sk | As-of price and category |
| fact_sales | dim_store | store_sk | |
| fact_sales | dim_channel | channel_sk | |
| fact_sales | dim_promotion | promo_sk | Single-direction, promo optional |
| fact_sales | dim_date | order_date_key = date_key | |
| fact_returns | dim_date | return date | Role-playing date if you import dim_date twice |
| fact_inventory_snapshot | dim_product | product_sk | Closing snapshot as-of |
| fact_inventory_snapshot | dim_store | store_sk | |

Filter `dim_customer[is_current]` on a disconnected current-attribute table when a visual must show today's tier rather than the tier on the sale. Do not replace the fact's `customer_sk` with the current key or Friday's sale will pick up Sunday's tier.

## Measures

```dax
Net Sales = SUM ( fact_sales[net_sales_amt] )

Gross Sales = SUM ( fact_sales[gross_sales_amt] )

Orders =
CALCULATE (
    DISTINCTCOUNT ( fact_sales[order_id] ),
    fact_sales[is_cancelled] = FALSE ()
)

AOV = DIVIDE ( [Net Sales], [Orders] )

Units Ordered =
CALCULATE (
    SUM ( fact_sales[qty_ordered] ),
    fact_sales[is_cancelled] = FALSE ()
)

Units Returned = SUM ( fact_sales[qty_returned] )

Return Rate = DIVIDE ( [Units Returned], [Units Ordered] )

Refund $ = SUM ( fact_returns[refund_amt] )

Cancelled Orders =
CALCULATE (
    DISTINCTCOUNT ( fact_sales[order_id] ),
    fact_sales[is_cancelled] = TRUE ()
)

Sell-through = AVERAGE ( mart_inventory_health[sell_through] )

Days of Supply = AVERAGE ( mart_inventory_health[days_of_supply] )

Stock-out Days = SUM ( mart_stockout[stockout_days] )

Inventory Variance Qty = SUM ( inventory_variance[variance_qty] )

COGS = SUM ( fact_sales[cogs_amt] )
```

`Sell-through` and `Days of Supply` averaged from the mart are fine for a card only with a single sku/store filter. A matrix should use the mart columns directly so it does not average ratios.

RFM is `mart_customer_rfm` (`r_score`, `f_score`, `m_score`, each 1–5 with 5 best when the customer count fills five tiles). Cohort is `mart_cohort`.
