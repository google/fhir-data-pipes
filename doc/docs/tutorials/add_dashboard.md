# Visualize Parquet DWH with Apache Superset

Generating dashboards is a common task for many digital health projects. 

This tutorial shows you how to use the popular open source [Apache Superset](https://superset.apache.org/) package to build a dashboard on top of the "lossless" parquet DWH created via the FHIR Data Pipes [Single Machine Deployment Tutorial](../tutorial_single_machine).

The principles here will apply to **any** visualization or BI tool that has a hive connector.

## Prerequisites

*   A FHIR Data Pipes single machine deployment using [local test
    servers] as the data source
*   Run the full pipeline at least once before following this guide.

## Install Apache Superset

There are [several ways to install Superset](https://superset.apache.org/). A simple option is to [install Superset locally using docker-compose](https://superset.apache.org/docs/installation/docker-compose/#installing-superset-locally-using-docker-compose). Once it is set-up, log in by visiting http://localhost:8088/ and using the default credentials username: `admin`
password: `admin`.

## Connect to the Apache Spark SQL data source

1.  In the upper-right corner, select **Settings > Database Connections**.
2.  In the upper-right corner, select **+ Database**.

## Create the connection for Spark SQL

1.  Under **Supported Databases**, select **Apache Spark SQL**.
2.  Under **Display Name**, enter "Single Machine Test Data" or another name
    your choice.
3.  Under **SQLAlchemy URI**, enter the following:

    ```
    hive://hive@<IP_address_of_docker_network_gateway>:10001
    ```

    To find the IP address, run:

    ```shell
    docker network inspect bridge --format='{{json .IPAM.Config}}'
    ```

4.  Select **Test Connection**. You should see a pop-up in the bottom-right
    corner that says "Connection looks good!"
    *   If you get an error, double-check the IP address is correct. You may
        also need to [install the Apache Spark SQL database drivers][6].
5.  Select **Connect**. Although you see an error: "An error occurred while
    creating databases: Fatal error" the connection is still created.
6.  Close the **Connect a database** window by clicking outside of it and
    refresh the **Databases** page. The database you just created should appear.

## Create a dashboard

Create an empty dashboard so you can add charts to it as they are created.

1.  At the top of the Superset page select the **Dashboard** tab, then click **+
    Dashboard**.
2.  Select the title area which reads **[ untitled dashboard ]** and give the
    dashboard a name. The examples below use "Sample Dashboard".
3.  In the upper-right corner, select **Save**.

## Query the database and generate Charts

1.  From the top tabs, select **SQL > SQL Lab**.
2.  If this is your first time using SQL Lab, you should be in a blank, empty
    tab. If not, click **New tab** or press `Control+T` to create one.
3.  On the left side under **Database**, select the database you created
    earlier, for example **Single Machine Test Data**.

### Add a Big Number

This first chart shows the total number of patients as a "Big Number".

1.  Replace the placeholder query `SELECT ...` with the following:

    ```sql
    SELECT COUNT(*) FROM Patient;
    ```

2.  Click **Run**. After a moment, the results of the query should appear.
3.  In the **Results** section, click **Create Chart**. This brings you to a new
    chart page.
4.  Under the **Chart Source** pane, click **Create a dataset** and name it
    "Patient Count Dataset".
5.  Under the **Data** tab, switch to the **4k (Big Number)** chart type.
6.  In the **Query** section, click the **Metric** section and set the following
    values:
    1.  Column: `count(1)`
    2.  Aggregate: `MAX`
7.  Click **Update Chart** at the bottom of the pane.

The chart updates to show a single large number of the count of patients.

8.  Click **Save** to bring up the **Save chart** dialog. Name the chart
    "Patient Count" and under **Add to Dashboard** select the dashboard you
    created previously.

### Add a pie chart

The next chart shows the split of patients by gender as a pie chart.

1.  Navigate back to SQL Lab. You should see the previous patient count query.
    *   If you'd like to keep the patient count query, make a new tab and set
        the database to "Single Machine Test Data" or the name you chose.
2.  Enter the following as the query:

    ```sql
    SELECT `gender` AS `gender`,
           count(`gender`) AS `COUNT(gender)`
    FROM Patient AS P
    GROUP BY `gender`
    ORDER BY `COUNT(gender)` DESC;
    ```

3.  Click **Run**. After a moment, the results of the query should appear.
4.  In the **Results** section, click **Create Chart**. This brings you to a new
    chart page.
5.  Under the **Chart Source** pane, click **Create a dataset** and name it
    "Gender Split Dataset".
6.  Under the **Data** tab, switch to the **Pie Chart** chart type.
7.  In the **Query** section, set the following values:
    *   Dimensions:
        1.  Column: `gender`
    *   Metric:
        2.  Column: `COUNT(gender)`
        3.  Aggregate: `MAX`
8.  Click **Update Chart** at the bottom of the pane. The chart updates to show
    a pie chart of patients by gender.
9.  Click **Save** to bring up the **Save chart** dialog. Name the chart "Gender
    Split" and under **Add to Dashboard** select the dashboard you created
    previously.

### Add a complex bar chart

The next chart shows number of patients on HIV treatment by year and display
that as a bar chart over time. The query is provided below.

* Navigate back to SQL Lab. 
* You should see the previous patient count query. 
* If you'd like to keep the patient count query, make a new tab and set the database to "Single Machine Test Data" or the name you chose.

Enter the following as the query (as either SQL-on-FHIR or using the pre-defined observation_flat views):

=== "Using SQL-on-FHIR Query"

    ```sql
    SELECT COUNT(*), YEAR(O.effective.dateTime)
    FROM Observation AS O LATERAL VIEW EXPLODE(code.coding) AS OCC LATERAL VIEW EXPLODE(O.value.codeableConcept.coding) AS OVCC
    WHERE OCC.code LIKE '1255%'
      AND OVCC.code LIKE "1256%"
      AND YEAR(O.effective.dateTime) < 2023
    GROUP BY YEAR(O.effective.dateTime)
    ORDER BY YEAR(O.effective.dateTime) ASC
    ```
=== "With pre-defined flat views"

   ```sql
   SELECT COUNT(*), YEAR(o.obs_date)
   FROM observation_flat as o
   WHERE o.code LIKE '1255%'
     AND o.val_code LIKE "1256%"
     AND YEAR(o.obs_date) < 2023
   GROUP BY YEAR(o.obs_date)
   ORDER BY YEAR(o.obs_date) ASC 
   ```

`The codes 1255% and 1266% refer to HIV\_Tx codes in the dataset`

* Click **Run**. After a moment, the results of the query should appear.
* In the **Results** section, click **Create Chart**. This brings you to a new
    chart page.
* Under the **Chart Source** pane, click **Create a dataset** and name it "HIV
    Treatment Dataset".
* Under the **Data** tab, switch to the **Bar Chart** chart type.
* In the **Query** section, set the following values:
    
    X-Axis: Custom SQL: `YEAR(date\_time)`
    
    Metric: Column: `patient\_id`, Aggregate: `COUNT`
    
* Click **Update Chart** at the bottom of the pane. The chart updates to show
    a bar chart of patients undergoing HIV treatment per year.
* Click **Save** to bring up the **Save chart** dialog. Name the chart "HIV
    Treatment" and under **Add to Dashboard** select the dashboard you created
    previously.

## Edit the Dashboard

Navigate to the **Dashboards** section and select the Sample Dashboard you
created. You should see something like this with the charts you created.

![Initial dashboard][init-dash]

Create a dashboard with 2 rows and a header. First click **Edit Dashboard**
then:

1.  In the right pane select the **Layout Elements** tab.
2.  Add a second row by dragging a **Row** element onto the canvas below the
    existing row.
3.  Arrange the charts to have the Patient Count and Gender Split in the top
    row, and HIV Treatment in the bottom row.
4.  Resize the top charts to use half of the width by dragging the edge of the
    chart.
5.  Resize the HIV Treatment chart to take up the whole width of the row.
6.  Add a header element at the top and enter "Key Program Metrics".

Your dashboard now looks something like this:

![Finished dashboard][fin-dash]

## Update the dashboard

To check that the dashboard is working properly, add a new patient to the FHIR
Store and run the incremental pipeline.

1.  Add a new patient to the server:

    ```
    curl -X POST -H "Content-Type: application/fhir+json; charset=utf-8" \
         'http://localhost:8091/fhir/Patient/' -d '{"resourceType": "Patient"}'
    ```

2.  Alternatively, use the HAPI FHIR server tester; go to [the Patient resource
    tester][8], click the **CRUD Operations** tab, paste the following resource
    into the **Contents** field of the **Create** section, then click
    **Create**.

    ```
    {"resourceType": "Patient"}
    ```

3.  Open the Pipeline Controller at http://localhost:8090 and run the
    incremental pipeline.
4.  Once the pipeline has finished, go to Superset and open your dashboard.
    Click the **...** button next to **Edit Dashboard** and then click **Refresh
    dashboard**. You should see the patient count increase by 1.

## Learn more

For more information about Superset, go to the [Superset documentation][4].

[1]: https://superset.apache.org/
[2]: https://github.com/google/fhir-data-pipes/wiki/Analytics-on-a-single-machine-using-Docker
[3]: https://github.com/google/fhir-data-pipes/wiki/Try-the-pipelines-using-local-test-servers
[4]: https://superset.apache.org/docs/intro
[5]: https://superset.apache.org/docs/installation/installing-superset-using-docker-compose#installing-superset-locally-using-docker-compose
[6]: https://superset.apache.org/docs/databases/installing-database-drivers
[7]: https://github.com/google/fhir-data-pipes/wiki/Analytics-on-a-single-machine-using-Docker#best-practices-for-querying-exported-parquet-files
[8]: http://localhost:8091/resource?serverId=home&pretty=false&\_summary=&resource=Patient&resource=Patient

[init-dash]: https://github.com/google/fhir-data-pipes/assets/7772901/eba8cbfb-6c5e-431f-8bcb-a9290d95f157
[fin-dash]: https://github.com/google/fhir-data-pipes/assets/7772901/90e8649a-6b78-46d0-8a17-69979f7353af