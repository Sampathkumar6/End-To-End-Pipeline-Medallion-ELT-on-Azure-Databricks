### End-To-End_Pipeline: Medallion ELT on Azure Databricks
#### Overview
End-To-End_Pipeline is a Lakeflow Job. It moves raw e-commerce data (orders, products, customers) from Azure Data Lake Storage Gen2 through  **Bronze -> Silver -> Gold**  layers and builds a small star schema in Unity Catalog:
*  DimCustomers: customer dimension (SCD Type 1, with a surrogate key)
*  DimProducts: product dimension (SCD Type 2, built by a Lakeflow Spark Declarative Pipeline)
*  FactOrders: order fact table linked to both dimensions

| Item | Value |
| ------ | ------ |
| Job ID | 642276223292130 |
| Unity Catalog catalog | databrick_project2 (schemas bronze, silver, gold) |
| Storage account | databrickssamproject2 (ADLS Gen2) |
| Containers | source, bronze, silver, gold |
| Compute | Serverless (notebooks and pipeline), performance-optimized |
| Trigger | Manual (no schedule configured) |
| Concurrency | max_concurrent_runs = 1, queueing enabled |
| Notebook folder | /Workspace/Users/sampathbunty6@gmail.com/databricks_project2/ |
| Typical runtime | About 4-5 minutes end to end (last full run: 4m 19s) |

#### Architecture
```
ADLS source/{orders,products,customers}  (parquet files)
        |  Auto Loader (incremental, trigger once)
        v
ADLS bronze/{dataset}                    (parquet)
        |  batch clean & enrich
        v
ADLS silver/{dataset}                    (Delta, overwrite)  -> databrick_project2.silver.silver_*
        |
        +-- gold_customers  (notebook, SCD1 MERGE)  -> gold.DimCustomers
        +-- Gold_Products_Pipeline (SDP, SCD2)      -> gold.DimProducts
        +-- gold_orders     (notebook, MERGE)       -> gold.FactOrders

```

#### Security & Access Management (Azure IAM)
*   **Azure Entra ID User Admin:** The admin email is configured in `accounts.azuredatabricks.net`.
*   **Azure Databricks Connector:** Created the `databrick_project2_connector` (Access Connector for Azure Databricks).
*   **Role Assignment:** The connector is granted the **Storage Blob Data Contributor** role on the storage account (`databrickssamproject2`) using a Managed Identity. This allows Databricks to securely read and write to the ADLS containers.

#### Unity Catalog Integration
*   **Metastore Configuration:** A metastore container is added to ADLS and linked to the project workspace.
*   **External Storage Credentials:** An external storage credential is created in Unity Catalog, bound to the Azure Databricks Connector ID.
*   **External Locations:** External locations are mapped to all ADLS containers (`source`, `bronze`, `silver`, `gold`, `metastore`) to govern access.
*   **Schema Syncing:** Schemas in the `databrick_project2` catalog are synced directly to these external locations during creation.

#### Task graph

<table>
  <tr>
    <th align="center">Task Graph Architecture</th>
    <th align="center">Task Timeline Architecture</th>
  </tr>
  <tr>
    <td align="center"><img src="images/task_graph.png" width="300" alt="Task Graph"></td>
    <td align="center"><img src="images/task_timeline.png" width="300" alt="Task Timeline"></td>
  </tr>
</table>


```
Parameters
   +-- bronze_auto_loader (for-each: orders, products, customers)
          +-- silver_customers --+
          +-- silver_orders -----+-- gold-dim_customers --+
          +-- silver_products ---+-- gold_dim_products ---+-- fact_orders

```

| # | Task key | Type | Source | Depends on |
| ------ | ------ | ------ | ------ | ------ |
| 1 | Parameters | Notebook | parameters | none |
| 2 | bronze_auto_loader | For each -> notebook | bronze_layer | Parameters |
| 3 | silver_customers | Notebook | silver_customers | bronze_auto_loader |
| 4 | silver_orders | Notebook | silver_orders | bronze_auto_loader |
| 5 | silver_products | Notebook | silver_products | bronze_auto_loader |
| 6 | gold-dim_customers | Notebook | gold_customers | all 3 silver tasks |
| 7 | gold_dim_products | Pipeline | Gold_Products_Pipeline (gold_products) | all 3 silver tasks |
| 8 | fact_orders | Notebook | gold_orders | gold-dim_customers, gold_dim_products |

Every task uses run_if = ALL_SUCCESS and has no timeout or retries. A failure in any task skips everything after it.

#### Task details
##### 1. Parameters: dataset list
*  Defines the datasets to ingest: orders, products, customers.
*  Publishes them as the task value output_datasets with dbutils.jobs.taskValues.set.
*  To add a new source folder, add an entry to this list.

##### 2. bronze_auto_loader: Bronze ingestion (for each)
*  Runs bronze_layer once per entry in {{tasks.Parameters.values.output_datasets}}, passing file_name = {{input.file_name}}.
*  Reads abfss://source@.../{file_name} incrementally with  **Auto Loader**  (cloudFiles, format parquet). The inferred schema is tracked at bronze/checkpoint_{file_name}.
*  Writes  **parquet**  to abfss://bronze@.../{file_name} in append mode, using trigger(once=True) and a checkpoint at bronze/checkpoint_{file_name}.
*  Only new source files are processed on each run. Earlier files are skipped because of the checkpoint.

##### 3. silver_customers
*  Reads bronze/customers (parquet) and drops _rescued_data.
*  Renames state to state_iso_code.
*  Derives Domain (the part of email after @) and Fullname (first_name + " " + last_name), then drops first_name and last_name.
*  Writes Delta to silver/customers in  **overwrite**  mode. It is read downstream as databrick_project2.silver.silver_customers.
*  Output columns: customer_id, email, city, state_iso_code, Domain, Fullname.

##### 4. silver_orders
*  Reads bronze/orders (parquet) and drops _rescued_data.
*  Adds a year column from order_date.
*  Also includes window-function examples (row_number, dense_rank, rank per year, ordered by total_amount) wrapped in a windows class. These are only displayed and are  **not written** .
*  Writes Delta to silver/orders in  **overwrite**  mode, read as databrick_project2.silver.silver_orders.
*  Output columns: order_id, customer_id, product_id, order_date, quantity, total_amount, year.

##### 5. silver_products
*  Reads bronze/products (parquet) and drops _rescued_data.
*  Creates the Unity Catalog SQL function databrick_project2.bronze.discount_func(p_price), which returns p_price * 0.9.
*  Adds Discounted_price = discount_func(price).
*  Writes Delta to silver/products in  **overwrite**  mode, read as databrick_project2.silver.silver_products.
*  Output columns: product_id, product_name, category, brand, price, Discounted_price.

##### 6. gold-dim_customers: DimCustomers (SCD Type 1)
*   **Parameter:**  init_load_flag, passed as a task base parameter (currently "0").
    *  0: incremental. Existing keys are read from gold.DimCustomers.
    *  1: initial load. An empty "old" set is used and surrogate keys start at 1.
*   **Steps:**
    1. Reads silver.silver_customers and removes duplicates on customer_id.
    2. Left-joins to the existing dimension to split records into  **new**  and  **existing**  customers.
    3. Existing customers keep their dim_cust_key and create_date; update_date is set to now.
    4. New customers get create_date and update_date set to now, and dim_cust_key = max(existing key) + monotonically_increasing_id() + 1.
    5. The two sets are combined with unionByName and merged into abfss://gold@.../DimCustomers on dim_cust_key: matched rows are updated, unmatched rows inserted. If the table doesn't exist yet, it is created with saveAsTable("databrick_project2.gold.DimCustomers").
*   **Output columns:**  customer_id, email, city, state_iso_code, Domain, Fullname, dim_cust_key, create_date, update_date.

##### 7. gold_dim_products: DimProducts (SCD Type 2, Lakeflow Spark Declarative Pipeline)
Pipeline  **Gold_Products_Pipeline**  (86d8c8c9-36a1-4f3b-bba2-6bf9ca6fc14f): serverless, Photon, triggered, CURRENT channel, target databrick_project2.gold. Its only source file is the gold_products notebook.
silver_products fully overwrites its table on every run, so the pipeline treats silver as a  **snapshot**  instead of an append-only stream:
| Dataset | Type | Logic |
| ------ | ------ | ------ |
| DimProducts_snapshot | Streaming table (change data feed on) | Snapshot CDC from silver.silver_products, keyed on product_id, SCD Type 1. Compares each new snapshot with the previous one to infer inserts, updates and deletes. |
| DimProducts_snapshot_changes | Temporary view | Streams the change data feed of DimProducts_snapshot, excluding update_preimage rows. |
| DimProducts_stage | Streaming table | Auto CDC (SCD Type 1) from those changes, sequenced by _commit_version, with deletes applied.  **Expectations (drop):**  product_id IS NOT NULL, product_name IS NOT NULL. |
| DimProducts_view | Temporary view | Batch read of DimProducts_stage. |
| DimProducts | Streaming table | Snapshot CDC from DimProducts_view, keyed on product_id,  **SCD Type 2** . Adds __START_AT / __END_AT; current rows have __END_AT IS NULL. |

The extra snapshot table exists because expectations aren't supported on a snapshot-CDC target. Changes are therefore passed through a change feed into DimProducts_stage, where the expectations apply.

##### 8. fact_orders: FactOrders
*  Reads silver.silver_orders.
*  Looks up dim_cust_key from gold.dimcustomers by customer_id, and dim_prod_key from gold.dimproducts. The product key is the natural product_id.
*  Uses left joins and drops the natural keys and helper columns.
*  Merges into databrick_project2.gold.factorders on order_id + dim_cust_key + dim_prod_key: matched rows are updated, unmatched rows inserted. On the first run it creates the table at abfss://gold@.../factorders.
*   **Output columns:**  order_id, order_date, quantity, total_amount, year, dim_cust_key, dim_prod_key.

#### Data objects

<table>
  <tr>
    <th align="center">ADLS Containers</th>
  </tr>
  <tr>
    <td align="center"><img src="images/adls_containers.png" width="400" alt="ADLS Containers"></td>
  </tr>
</table>

<table>
  <tr>
    <th align="center">Bronze & Gold Schemas</th>
    <th align="center">Silver Schema</th>
  </tr>
  <tr>
    <td align="center"><img src="images/catalog_1.png" width="300" alt="Bronze and Gold Schemas"></td>
    <td align="center"><img src="images/catalog_2.png" width="300" alt="Silver Schema"></td>
  </tr>
</table>

| Layer | Object | Format / location |
| ------ | ------ | ------ |
| Source | source/{orders,products,customers} | Parquet files |
| Bronze | bronze/{dataset}, bronze/checkpoint_{dataset} | Parquet plus Auto Loader checkpoints |
| Silver | silver.silver_customers, silver.silver_orders, silver.silver_products | Delta at silver/{dataset} |
| Gold | gold.DimCustomers | Delta at gold/DimCustomers |
| Gold | gold.DimProducts, gold.DimProducts_stage, gold.DimProducts_snapshot | Pipeline-managed streaming tables |
| Gold | gold.factorders | Delta at gold/factorders |
| UC function | bronze.discount_func(double) | SQL UDF |

#### Running the job

```bash
# Full run
databricks jobs run-now 642276223292130

# Re-run only failed tasks (and their downstream tasks) of a failed run
databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks

```
*   **First-ever load of DimCustomers:**  set the gold-dim_customers base parameter init_load_flag to "1" for that run, then set it back to "0".
*   **Full refresh of the product dimension:**  select by file path (gold_products) rather than by dataset name.

#### Operational notes and known limitations
*   **Environment constraints:**  the metastore has no root storage credential, and the catalog's default schema has no managed location. Pipelines must target the gold schema.
*   **gold_products notebook:**  its cells only work inside the pipeline. The errors shown in its cells come from running them directly in the notebook and can be ignored.
*   **Silver overwrites:**  every silver table is fully rewritten on each run. Anything that streams from silver must treat it as a snapshot, as DimProducts does.
*   **DimCustomers surrogate keys:**  monotonically_increasing_id() produces unique but  **non-contiguous**  keys.
*   **FactOrders product key:**  dim_prod_key is the natural product_id, joined against the full SCD2 history in DimProducts. Once a product has several versions, orders will duplicate. Join only current rows (__END_AT IS NULL), or do a point-in-time join on order_date.
*   **silver_orders rankings:**  the ranking columns are computed but never written. Persist them if they're needed downstream.
*   **silver_regions:**  this notebook is in the same folder but isn't part of the job.
*   **Reliability settings:**  there are no retries, timeouts or failure notifications. Consider adding email or webhook alerts and a schedule.

#### Change history (2026-11-02)
*   **gold-dim_customers task:**  added the base parameter init_load_flag = "0". This fixes InputWidgetNotDefined; the notebook never defined the widget.
*   **Gold_Products_Pipeline:**  rebuilt on snapshot CDC to fix DELTA_SOURCE_TABLE_IGNORE_CHANGES, which happened because silver is overwritten on every run. A one-time full refresh rebuilt the product tables.
*   **Validation:**  full run 287438717740687 succeeded. DimProducts, DimProducts_stage and silver products each have 500 rows.