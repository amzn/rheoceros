# RheocerOS User Guide

RheocerOS as a Python framework is designed to be the swiss-army knife of scientists and engineers who want to connect/transform (big-data, ML or other cloud) resources and orchestrate the flow between them with high-level of abstraction using a wide range of endpoints such as dev-desktops, notebooks.

It allows you to create/update a (generic data-science) application runtime in AWS with a single line of Python code. Then, you start pushing your artifacts, business logic or description of your external/remote artifacts into this application and connect them to each other. With application provisioning this easy for you and other RheocerOS users, you start connecting big-data resources between your own applications or by referencing others’ RheocerOS applications (from same/other account/s or region/s).

## High-Level Workflow

Application development or experimentation using the framework:

![Workflow](RheocerOS_Whatisit.drawio.png "High Level Workflow")


## Application Development in a Nutshell (API Flow)

Each block below represents a method from AWSApplication class (e.g AWApplication::create_data, AWSApplication::execute):
[Image: RheocerOS_Development_Experience.drawio.png]

With RheocerOS, you create DAGs of nodes (that encapsulate datasets, models, etc) within an application which might just be represented as simple as an object instance in your local Python environment. Once you activate it, it is a persistent auto-generated/updated, isolated/sandboxed NAWS application that runs in the background in a specific AWS account-region, with a specific app-name. Later on, you interact and update the logical topology of this NAWS application by using a local RheocerOS application object in the same or any other compatible Python environment (dev-box, Sagemaker notebook, Eider). You just need to know your (1) AWS account ID, (2) region and (3) application name to attach to it.

And when you want to build high-level graphs, topologies of those applications (for architectural reasons or maybe just for dataset, model sharing purposes), RheocerOS allows you to bind them together. Once bound, nodes from other (upstream) applications are discovered and used the same way as local nodes created within your own application and similarly your nodes in any other downstream application. Signal-propagation, triggers, computations are all handled by RheocerOS AWS applications running in the background.

It is an opinionated take on the entire data engineering / ML space that expands a team’s ability to create data (model) flows beyond conventional realms, do rapid experimentation and migrate applications, notebooks into AWS. 

One thing that you might want to know as a user is that it internally adapts a “signal-slots” approach around any entity related to big-data development. Datasets, models, timers are all RheocerOS entities that emit signals and receive other signals into their compute slots. The way those entities are used in an RheocerOS application Python code is pretty much the same. By using RheocerOS APIs, you implicitly create new signals and bind them to slots which in turn create other signals. While doing that, you just focus on input/output relationships, dimensional (e.g partitions) compatibility, dimensional filtering between those inputs and outputs, (optionally) metadata enrichment. The rest flows naturally at both dev-time and runtime.

Every time you activate your local RheocerOS application object (which is a DAG of nodes that each contain input signals, an output signal, dimensional filtering rules and one or more compute declarations), it provisions or updates an event-driven application in your target platform (AWS account) that comprises a logical processor, internal storage, batch-compute unit, ML-compute unit, routing mechanisms that all abstract underlying AWS resources, plumbing, single account, cross-account permission setup for you. You can then access/attach, modify the topology of your application from another environment and also link it to other RheocerOS applications to share data, model and metadata.
[Image: image.png]
While the application is running in your account, you can locally access its nodes (that might logically represent partitions of datasets, models), also routing information (active executions, historical executions) via the APIs provided by the framework. You can provision your app in a production pipeline but later on can analyze it, peek into its continuously updated partitions in a notebook. It is “write anywhere, run anywhere”.


## Feature List

* Data pipeline creation/flow management and integration capabilities. Below are the ways you create new Signals in an Application. These actions let you build DAGs of nodes. Later on, you can share them with other applications as well.
    * Seamless ingestion from external Andes data in NAWS. Ability to add custom metadata. Table updates (DELTA, SNAPSHOT) are auto-transformed into Signals and cause triggers in your downstream nodes. During compute, incoming Signal is available to you as a Spark Dataframe. NAWS-CDO access (compaction) and partition/primary keys will be already applied to the final Dataframe.
    * Seamless ingestion from external S3 data. Ability to add custom metadata. Partition updates are auto-transformed into Signals and cause triggers in your downstream nodes. When you are importing/marshaling an external S3 data, you usually provide basic S3 params (account id, bucket), partitioning scheme (keys, success file, etc). Partition updates are given as Spark Dataframes to user’s Compute code.
    * Dependency check against external data partitions (e.g S3, Andes).
    * End-2-End development: pipeline/workflow level unit-tests, integ-tests right out of the box. 
        * Development paradigm allows easy implementation of application specific end-2-end mechanisms:
            * Backfillers
            * Front-ends (web/desktop UI) to visualize application topology
    * Scheduling support, in case some nodes cannot be fully satisfied with the implicit event-driven scheme. Input time as a signal along with other input signals and control the final trigger. AWS EventBridge Rule syntax can be used to define CRON or more trivially ‘Rate’ based scheduling.
    * Create new data: aka _internal_-data.
        * Use signals (Andes, S3, Timer, _internal_) from the same or other (imported) applications and create a new Signal that represents another internal data.
        * Ability to define extra dimensional filtering on input signals (band-pass filter). Ex: one of your inputs (Signal) can be from a domain in which its ‘region_id’ (LONG) and ‘day’ (DATETIME) dimensions span [1,2,3,4,5] and ‘*’ (any) filtering specs / values. You can easily create a band-pass filter to narrow down your binding to 1 (NA region) only and last 30 days by an intuitive operation such as “input[1][-30]”.
        * Define the trigger condition for the execution of your ‘Compute’ targets. It is called ‘dimensional linking’ which allows you to describe how your input signals relate to each other on a dimensional (i.e partition) basis. A logical operation such as: *input1[‘region’] == ID_TO_NAME_MAP.get(input2[‘region_id’]) AND input1[‘’ship_day] == input2[’day‘].strftime(****'%d-%m-%Y'****).*
        * Compute targets: user code-blocks (Python, PySpark, Scala, etc) that react to the readiness of inputs for the data node.
            * Declare _multiple_ compute targets for same trigger condition (any combination of the following two types):
                * Batch-compute / Spark code (in PySpark, Scala), PrestoSQL: data inputs are automatically converted to data-frame variables for user Spark code. Concrete dimension values for the output partition and timer value (if any) are provided as local variables. Input dataframes are created/unioned using the dynamic partition ranges (based on the underlying storage and dimension values). Final dataframe assigned to the ‘output’ variable gets persisted as the output partition. Dimension/partition management is handled automatically (even for Andes inputs).
                * Inlined-compute, a python function that can be called/executed within remote (AWS Lambda) processor: runtime version of data and timer inputs (with specific partition/dimension values) are available for user’s inlined code. Similarly an AWS session and other variables are passed so that user code can process the trigger condition in a totally custom way and access other AWS services.
                * Model-compute (**BETA**): e.g Sagemaker Training job, Batch-transform. Yes, even these operations are uniformly encapsulated as Signals, their domains are described with dimensions (partitions) and their outputs as ‘data’ which then can be hosed into another data (dataset or model node) or an inference-endpoint node(**V1.0**).
        * Completion of all of the compute targets for a data (node, route) automatically triggers new signals for its downstream dependencies.
        * Orchestration for the execution of compute targets, and also tracking their status, completion detection/handling and recording as historical records are all handled by RheocerOS in your AWS account.
    * Collaboration between different applications. Easy data/metadata sharing, permission and binding of different pipelines in a declarative and zero-config way.
    * Unified data discovery (listing, querying) scheme to find datasets , models from the same application and/or from other imported/upstream applications (while collaborating).
    * Dynamic flow management commands:
        * Programmatically inject raw events or emulate upstream partition creation events very easily: manual testing, integration testing, backfilling, etc.
        * Query, list active routes (e.g data nodes), executions and get very detailed / near real-time information in a structured format.
        * Query, list inactive (historical) routes, executions.
    * Easy infrastructure (packages, pipelines, etc) creation and rapid development. Currently encapsulated / documented within ‘new_app_template’ folder of RheocerOS package. For brand new projects (like Blade as a new application), artifact creation is necessary but for shared pipelines (applications) such as upcoming data-catalog, infrastructure is ready for all of the consumers. There are some easy but still manual steps to create a brand new package and pipeline to start the development of the application in an isolated way. Please refer [Productionization Support Cont’d:: RheocerOS - RoadMap](https://quip-amazon.com/TGlxAEndWjx7#ffV9CAVZpK0) in our roadmap to further enhance this for new project that would require a separate productionization effort and also an infrastructure.
* Cross-platform Python framework that enables access into same application instance from all of the supported runtime environments (Amazon/Non-Amazon dev-boxes, Apollo Environments, Sagemaker Notebook, Eider). See the related use cases below for more details.
* ML Workflow extensions with nodel build/inference APIs: TODO (ETA EoY 2022)  [3.4 Model Development Extensions: RheocerOS RoadMap 2022 (AI Foundations)](https://quip-amazon.com/7QXcAHXpbPct#temp:C:EUWd544e011411e9570ff08204a3)

## Environment Setup and Application Creation

### Application Specific Brazil Package

Step-by-step guide to create a new project (DEX C2P/QASP Project):

[Getting Started with QASP](https://quip-amazon.com/TQ2OAohbRsbC)

This is when you want to use your own Python modules in RheocerOS, in a more systematic way and also in a more productionization-ready way.

Please check [NEW_APP_STEPS](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/NEW_APP_STEPS) doc from RheocerOS/new_app_template folder and once you are done with the setups outline there check [NEW_APP_TODO](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/NEW_APP_TODO).

For example packages depending on RheocerOS, please check:


* [DEX-ML A-DEX (Blade) pipeline prototyping using RheocerOS](https://code.amazon.com/packages/RheocerOSBlade/trees/mainline)
* [DEX Data Catalog](https://code.amazon.com/packages/DexMLCommonDataCatalog)
* [ADEX (using IF)](https://code.amazon.com/packages/AdaptiveDeliveryExperienceMachineLearningIF/trees/mainline): Scala and Python ETL code in the same pipeline. Use this project template for your RheocerOS based workflows in which you’d like to use Scala and Python in a hybrid way.

If you have created your package already, you can use that package (instead of RheocerOS package) for the steps given in the next section for ad-hoc experimentation. 

### Initial Provisioning for Ad-hoc Experimentation

Depending on your use-case you might have to do one of the following to start your experience with RheocerOS.

_Note 1_: for dev-box related cases, if you prefer using PyCharm, please check [PYCHARM_SETUP](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/PYCHARM_SETUP.md) guide from RheocerOS package first. 
_Note 2_: If you want to experiment with RheocerOS outside of Brazil (on your Windows, Mac, etc), then follow “Development / Setup Dev-Env / Debugging / section (2)” from the [DEVELOPMENT](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/DEVELOPMENT.md) doc. Instructions in the rest of the doc given for dev-box below can easily apply to this scenario as well.

* Python **3.7** env setup via Miniconda and then using ‘requirements.txt’ from the root level of [RheocerOS package](https://code.amazon.com/packages/RheocerOS/trees/mainline) should be enough.

Irrespective of how you are going to use RheocerOS in different endpoints, you need to create your logical application first. Where you are going to create your application first will only be important for minor IAM role related steps if you want to access your application from other endpoints (Sagemaker, Eider). For majority of cases where you need a logical application to unblock your development experience on a notebook, this section provides the necessary initial steps.

**First time on a Dev-box:** 

* Create your Brazil work-space for RheocerOS.
* Pull [RheocerOS](https://code.amazon.com/packages/RheocerOS/trees/mainline)

* Modify any of the sample application Python files from ‘example’ folder to remove all of the lines following the instantiation of ‘AWSApplication’.
    * If you want to use Midway (without specifying any credentials via Odin or as access-pair, etc), then refresh your cookie: run ‘mwinit -o’. Midway mode is when you provide ‘account_id’ to the AWSApplication.
    * If you want to use default or explicit credentials to instantiate AWSApplication, then create an _admin_ user in the target AWS account and then either;
        * use its AWS provided credentials to update your ~/.aws/config, if you want to instantiate AWSApplication with name and region params only (acc_id will picked up from the session).
        * or create the user and Odin MS using [https://access.amazon.com](https://access.amazon.com/) and then use the following prologue code to init AWSApplication object (_Warning_: recommended for seamless transition into Eider):

```
import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from pyodinhttp import odin_material_retrieve
import bender.config

flow.init_config()
log = flow.init_basic_logging()

sc = bender.config.ServiceConfig()
odin_ms = sc.get(app_name, 'creds')['odin']

app = AWSApplication(app_name=YOUR_APP_NAME, # 'my-foo-app'
                     region=AWS_REGION, # 'us-east-1'
                     access_id=odin_material_retrieve(odin_ms, 'Principal').decode('utf-8'),
                     access_key=odin_material_retrieve(odin_ms, 'Credential').decode('utf-8'))
```

* Change your app-name, AWS acc and region ID.
* Run it via:
    * $ brazil-runtime-exec python examples/{sample_app}.py

Once the execution is over (with exit code ‘0’), bare-bones provisioning of your app is done with an internal storage (S3, etc) and necessary role setup (dev-role). 

_Note_: Admin role is used for initial check and creation of your dev-role (based on the requirements from underlying drivers) and never used to do anything else during development time or at runtime. RheocerOS operates on assumed dev-role even during dev-time. Admin role is suggested for your convenience only.

_Note_: Advanced initialization mode of applications is not covered in this document. But curious users who might intend to exploit the fluent-API of the underlying configuration object for an application to choose another AWS driver or to pass credentials directly can check this [sample code](https://code.amazon.com/packages/RheocerOS/blobs/158732363ffbf18f328bff186ec18452ea7d9781/--/examples/eureka_main.py#L53). Fluent API is provided by the builders of [AWSConfiguration](https://code.amazon.com/packages/RheocerOS/blobs/a35f547f5b99b1b1441dfaff883a32cfd34f14f9/--/src/intelliflow/core/platform/development.py#L331) and [Configuration](https://code.amazon.com/packages/RheocerOS/blobs/a35f547f5b99b1b1441dfaff883a32cfd34f14f9/--/src/intelliflow/core/platform/development.py#L110) classes from the framework. 

At this point, you can actually start adding new nodes to your application by calling APIs on the new ‘app’ object if you are on Python interactive shell. Or your can modify this file and run it again. It is like your ‘infrastructure in code’, representation of your data science application with no effect on AWS so far. 


```
new_data = app.create_data(inputs, ....)

new_data2 = app.create_data([new_data + some other inputs], ...)

...
```


if you want to make this flow active and running in AWS, and also you want it to keep listening for upstream input events (or using timers) and run the compute blocks from ‘create_data’ calls above, then you make the following call;

```
app.activate()
```


This is a synchronous call that initiates a provisioning chain in your local Python env against remote AWS resources. At the end of your this API call, you will have multiple AWS resources provisioned and connected to each other.

This is the full-scale initial provisioning of an RheocerOS app. There are so many different workflows that should be covered around such a simple state of this application. And also there are so many other APIs that change your application logic, topology and development experience by a huge margin. And more importantly, utilization of some depend on your personal preferences. We will expand on these (APIs) and provide more details in the ‘use-cases’ section.

Please also note that [Application Specific Brazil Package](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAvzzOD) section below is a super set of this section, meaning that if you already know how to build applications with RheocerOS and have other dependencies and also your own package (that depends on RheocerOS), then you can totally ignore this initial provisioning section. Even if you have already used IntelilFlow to provision your application, then when you can still use the same application to update it with the dependency closure of your own versionset within your app-specific package.


### Continue in Eider

Once your application is provisioned (storage and role setup is done) even without ‘activate’ call, then you do the following to switch to Eider for the rest of your experience.


```
 app.platform.sync_bunle()
```


This will make sure that your application’s dependency closure (working set) is cloned in remote storage which will be accessible by Eider thanks to the following actions:


* Clone https://eider.corp.amazon.com/yunusko/notebook/NBEDS8Y3ZVFC
* Modify the first cell and ‘AWS Account’ (from the right panel) and use the Odin MS that you’ve created in [First time on a Dev-box:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CANam7r).
    * If you did not use Odin based approach, then follow the steps from that section to create an admin user and Odin MS.
        * Now you have to modify your app’s dev-role and add new admin user as a trustee:
            * Go to your AWS account and add new admin IAM user as a trustee to ‘*arn:aws:iam::{AWS_ACC_ID}:role/{APP_NAME}-{AWS_REGION}-RheocerOSDevRole*’. Ex: *arn:aws:iam::286245245617:role/dc-client-qianshan-us-east-1-RheocerOSDevRole*
* Modify the Odin MS to make it accessible by POSIX user "nobody" on hostclass "EIDER-APP".
* Modify the second cell according to your app-name, account id and region:

```
 eider.s3.download("s3://if-{APP_NAME]-{AWS_ACC_ID}-{AWS_REGION}/Bundle/bundle.zip", "/tmp/bundle.zip")
```

* Modify the third cell according to your app-name.

Now you can use any new cell to keep accessing, modifying your application or use nodes from other applications that authorized your application.

_Note_: When you want to productionize (code review, etc) or test your code in any other RheocerOS env, then you just need to copy your application code into a new application package (recommended). Please check [Application Specific Brazil Package](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAvzzOD) section below for that.

_Note_: When you activate your application in Eider, the dependency closure (bundle.zip) created from the originator Brazil package will be used.


### Continue in Sagemaker

Transition into Sagemaker to keep developing in that environment is more seamless than Eider. To provision your own Sagemaker Notebook instance and get an URL for it, please check the following example application:

[Remote notebook provisioning example in RheocerOS/examples](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/examples/remote_notebook_provision.py)

Either you can click on the presigned URL printed on the console or can now go to AWS Sagemaker Console / Notebook Instances and check the newly created/updated notebook instance specific to your app.

You can keep developing in Sagemaker and add dependencies, when you activate your app there, new dependency closure for your app will be determined by the site-packages of your active kernel (‘RheocerOS’ kernel) depending on whether you pip’d or conda’d new packages over there.

When you want to productionize via [Application Specific Brazil Package](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAvzzOD), then you might want to add those dependencies to your package’s Config file. Note: this might require other modifications (merge, etc) in your app specific version-set.

_Note_: If you have already provisioned a Sagemaker notebook via ***Application::provision_remote_dev_env(endpoint_args)***

and also want to use Eider, then you can actually skip ***app.platform.sync_bundle()*** call for Eider.

### Converting Data Partitions into Other Popular Formats

A key / integral part of your experience in a notebook is to go out of the loop (RheocerOS app’s topology) and do experimentation within the execution context of your cells and use other libraries (Pandas, Spark, etc).

It greatly benefits the experience with RheocerOS as well, particularly for big-data code development. You develop, test and analyze your Spark code for example, in a cell outside of RheocerOS app’s context, then when you want to make it as part of your pipeline and get automatically updated, you just add a node via ‘create_data’ API.

For that purpose, RheocerOS provides simple APIs (load_data, preview_data) to load or preview data based on your preferences (limit, format, etc). Please see the section [Data Analysis](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAQLdNc) for more details and examples.

Or you will use ‘materialize’ API to get the physical path to a particular or range of partitions that map to a node and directly rely on other libraries (Pandas, Spark, etc) to load the data in a different manner.

Examples:

```
`# get the reference to a dataset from an imported upstream application (aka data-catalog)`
`all_sog_data_NA `**`=`**` catalog_app``[``"all_ship_option_group_recommendations_NA"``]`

# get physical path for NA region (id = 1) and date = '2020-11-10'
`paths `**`=`**` app.materialize(all_sog_recs[1]['2020-11-10'])

pandas_dataframe = pd.load_csv(path[0])`
```

or read a range:

```
`# get the physical paths for the last two days using '2020-10-29' as tip, in region=1
paths `**`=`**` app.materialize(all_sog_data_NA[1][:`**`-`**`2], [1, '2020-10-29'])

# create Spark dataframe's for each path
# create Spark code based on paths`
```

Later on you can add your experimentation code in your app pipeline:


```
new_node = app.create_data(all_sog_data_NA[1][:-7],  # last 7 days of data
                           ...,
                           BatchCompute("your new spark code", ...))
```

### (Optional) Andes Access

Independent of your dev-endpoint of choice, if you want to use Andes tables in your applications, then for each AWS account that you want to use please go through these steps outlined and maintained in:

* [Andes Glue Sync setup for an AWS account that will be used by an Intelliflow app](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/ANDES_GLUE_SYNC_SETUP)

_Note_: This step is necessary even if you are just importing Andes based nodes/Signals into your application from other RheocerOS applications. When the actual data needs to be accessed (in Glue, etc), RheocerOS requires this sync mechanism to be setup.

Once you use an Andes table as an input to any of the nodes, then RheocerOS starts catching events for it and evaluates them as part of trigger analysis at runtime.


### (Optional) Bulk Provisioning / Notebook management for the Entire Team

This is important for a better onboarding and also continuous (binary) sync operation on scientist/engineer notebooks in bulk mode.

* Setup domain-specific RheocerOS package by following [Application Specific Brazil Package](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAvzzOD)
* Pay special attention to the [14th step](https://code.amazon.com/packages/RheocerOS/blobs/a35f547f5b99b1b1441dfaff883a32cfd34f14f9/--/new_app_template/NEW_APP_STEPS#L230) of [NEW_APP_STEPS](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/NEW_APP_STEPS) where it is mentioned that limit increases on some AWS resources might be needed (depending on your team size). Use the links provided there to get an idea and evaluate if you need to do something similar.
* Create a cruise-control application like  https://code.amazon.com/packages/DexMLCommonDataCatalog/blobs/mainline/--/application/catalog_clients.py
    * Modify its user-name list, app-name prefix, account id and region name fields.

When you run this executable, your remote notebooks will be binary compatible with the local Python environment of your package (dependency closure of your RheocerOS app to be more precise).

For systematic update of remote notebooks, turn this executable as an activation script in your production pipeline. This will guarantee that remote notebook environments will be code-consistent with your applications version-set and other details of your application.

This will take care of the binary compatibility your Eider and Sagemaker notebooks.


## Application Update / High Level

In any of the endpoints, if you want to modify your application, then there is one thing that you have to be aware of. It is the fact that each AWSApplication object represents the brand-new version (a new sctrachpad) for your application. So your application code can be run and activated again and again, but still yielding the same runtime topology. It is pretty much like CloudFormation from that perspective. Changeset-management, incremental updates/removals are handle by the framework (and its internal drivers) automatically as compared to the previous running version of your application. 

But at development time, you should treat a new AWSApplication object as a brand new page.

_Note_: This nature of AWSApplication allows us to use application code in our code-pipelines to productionize our solutions also. It is similar to the productionization scheme of Cradle applications in a way. 

Then, in the context of this fact, we have the following problem:

How can I access and modify my application remotely from other endpoints such as Notebooks where I don’t have the entire application code?

The answer to this question is the ‘attach’ mechanism. For various reasons, users might just need to attach their application to pull the active topology into the dev-time and extend it. So basically the ultimate effect of running the following code snippets are equivalent against the same application (running in AWS):

1- Entire application code:

```
app = AWSApplication('foo-app', 'us-east-1', '111222333444')

data1 = app.create_data(...)

data2 = app.create_data([data1 + ...], ...)

app.activate()
```


2- ***Application::attach()***: reload the activated state (DAGs of active nodes). This will allow the user to incrementally extend the pipeline by adding a new node.


```
app = AWSApplication('foo-app', 'us-east-1', '111222333444')

app.attach()

data2 = app.create_data([app['data1'] + ...], ...)

app.activate() 
```


When you are using the attach mechanism (when you don’t have the code that was previously executed), then obviously the local python variables for your upstream data-nodes are not available in the scope. So you have to reference them by using their IDs (or using various other ways such as querying, listing, etc) to pull them into the input scope of a new create_data call. In the example above, indexed access into ‘app’ object via ‘app[’data1’]  gives you the reference to the existing node for ‘data1’. For more details on this please refer the section [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND).

Of course, the above example assumes that the previously executed (running) state of the application was like:


```
app = AWSApplication('foo-app', 'us-east-1', '111222333444')

data1 = app.create_data(...)

app.activate()
```



## Application Update / New Nodes

### Data

RheocerOS regards datasets, models as data. So it has common APIs (to handle data related signalling, node generation) which has abstract parameter types to be specific for different datasets, models ,etc.

In the previous section, we intentionally avoided the details on how to use ‘create_data’ API, so that you can first get an idea about the high-level flow in an RheocerOS application.

And more importantly we completely avoided another API that helps you to introduce new nodes / signals for external data into your application. This other API is ‘marshal_external_data’.


***Application::marshal_external_data***: Unless you are [Connecting with other Applications](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAM0RqM) or using [Timers](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAUXRC2) as initial (event) seed to your application, then adding external data into your application is kind of mandatory.

_Note_: Once you introduce external data nodes into your application, then they are available as inputs, test-signals to the rest of your application code or downstream applications that import your application, pretty much the same way other nodes (created by ‘create_data’ or ‘create_timer’) are used.


Synopsis:

```
    def marshal_external_data(self,
                              d: ExternalDataNode.Descriptor,
                              id: str,
                              dimension_spec: Union[DimensionSpec, Dict[str, Any]],
                              dimension_filter: Union[DimensionFilter, Dict[str, Any]],
                              protocol: SignalIntegrityProtocol = None,
                              tags: str = None
                              ) -> MarshalerNode:
```


**Params**:

_external_data_desc_: describe the “resource” root path and partition format/pattern that includes dimension which are used during the partitioning of the external data.  Other resource specific details and metadata are provided here. This is the only resource specific param of this API. Possible implementation: *S3Dataset, AndesDataset*, etc
_id_ : internally unique ID of the dataset. you might use this *id* later to refer to this node in cases where you won’t have the returned value (<MarshalerNode>) in the context. Conflicts around these identifiers will raise validation errors from the framework. “id”s are mostly used when you import nodes from other applications. Please refer [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND) for more details on how “id” field is used.
_dimension_spec_: a hierarchical description of the dimensions (e.g partitions ,etc) of the entire domain that this new node/signal belongs to. It internally adapts a hierarchical data-structure that uses one or more [*Dimension*](https://quip-amazon.com/hCN1A8Z4uVHi/RheocerOS-Programming-Suite#LHQ9CA2zZc6)*s* at each level. User can programmatically build the *DimesionSpec* object or just use the raw (Python Dict[str, Any]) based representation for convenience. Please the section [Dimension Domain of a Signal…: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CAMs9B1) from the Core Framework for more details and theoretical introduction. Check the API specific examples section to get a better idea.
_dimesion_filter_: an optional (but highly recommended) specialization/instantiation of the “dimension_spec”. This is also your opportunity to create the root band-pass filter for this logical [Signal](https://quip-amazon.com/hCN1A8Z4uVHi/RheocerOS-Programming-Suite#LHQ9CAcLQmc). This filter  (of type *DimensionFilter*) determines the view of the downstream dependencies of this node/Signal (that use this as an input for example in ‘create_data’ API). If you are OK with an all-pass from the upstream raw data source, then you can leave this with the special char ‘*’ on all dimensions. In that case, any event from upstream source will be evaluated by downstream nodes. Check the API specific examples section to get a better idea. And also, please check the section [Dimension Filtering: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CAqpYJ3) from the Core Framework for more details and theoretical introduction. Similarly with ‘dimension_spec’, this param can also be built programmatically  as an *DimensionFilter* object or just as a raw (Python Dict[str, Any]) based representation for convenience.
_completion_protocol:_  what is the partitioning completion protocol used by the owner (upstream provider) of this dataset? RheocerOS provides many protocols but for the sake of simplicity and of most common use-cases, we will mention the FILE_CHECK here. You use the following protocols in majority of external data:

```
    SignalIntegrityProtocol("FILE_CHECK", {"file": [{FILE_NAME_1}, ..., {FILE_NAME_N}]}
```

      Protocol examples: 
      
Default for generic datasets (from S3): even if you leave the protocol empty, for external data descriptors like *S3Dataset* the following is the default. ‘_SUCCESS’ file might seem familiar to you since it is the default Hadoop success file being used by most of our Spark based clusters as well.

```
      SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}
```

          Default for Andes: even if you leave the protocol empty for a node that uses *AndesDataset*, then the following is set implicitly.

```
      SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}
```


          Default others (models, etc): TBD

**Returns**: bind the returned variable to downstream API calls such as [Application::create_data:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAZMmq5) or Application::materialize.

_[MarshalerNode](https://quip-amazon.com/hCN1A8Z4uVHi/RheocerOS-Programming-Suite#LHQ9CAE9kJH)_: Almost all of the RheocerOS data manipulation APIs will use this type as the return type. Users generally (and implicitly) use the APIs of this type for extra operations, local analysis and filtering on the dataset. 

When binding datasets (and other data) into other APIs, RheocerOS expects “marshaled” types like this as the input. For dataset APIs, MarshalerNode is the input-output format, binding factor across a chain of operations.

The other marshaled type which is almost %100 synthatically compatible with MarshalerNode is [Filtered View: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CA8GgEu). This type is the result of a series of indexed operators applied to a MarshalerNode (return value of create_data, marshal_external_data, create_timer, etc).


```
        external_data: MarshalerNode = marshal_external_data(....)
        new_filtered_view: FiteredView = external_data[dim_1_value_or_range][...][dim_N_value_or_range]
```


Example 1 (from S3):

```
eureka_offline_all_data = app.marshal_external_data(
    external_data_desc= S3Dataset("427809481713", # account
                                  "dex-ml-eureka-model-training-data", # bucket
                                  "cradle_eureka_p3/v8_00/all-data", # root folder
                                  "partition_day={}",  # variants/dimensions/partitions
                                  dataset_format=DataFormat.CSV
                                  # , other custom metadata as kwargs)
    , 
    id = "eureka_training_all_data"
    , 
    dimension_spec={
        'day': {
            'type': DimensionType.DATETIME
        }
    }
    , 
    dimension_filter={ 
        "*": { # '*' -> all-pass (evaluate) whatever partition source S3 folder emits
            'format': '%Y-%m-%d',  # this also determines to output resource path (ie: s3://.../.../2020-11-16)
            'timezone': 'PST',  # serves both as a manifest (for other users) and also metadata
                                # for RheocerOS to eliminate/remedy "impedance-mismatch" between data.
        },
    },
    # 'eureka_offline_all_data' will trigger downstream nodes (that use it as input)
    # when a signal with '_SUCCESS' physical resource is detected at runtime.
    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)
```


Example 2 (from Andes):


```
ducsi_data = app.marshal_external_data(
    AndesDataset("booker", 
                 "d_unified_cust_shipment_items",
                 partition_keys=["region_id", "ship_day"],
                 primary_keys=['customer_order_item_id', 'customer_shipment_item_id'],
                 table_type="APPEND")
    , "DEXML_DUCSI"
    , {
        'region_id': {
            'type': DimensionType.LONG,
            'ship_day': {
                'type': DimensionType.DATETIME
            }
        }
    }
    , {
        '1': {
            '*': {
                'format': '%Y-%m-%d %H:%M:%S',
                'timezone': 'PST',
            }
        },
        '2': {
            '*': {
                'format': '%Y-%m-%d %H:%M:%S',
                'timezone': 'GMT',
            }

        },
        '3': {
            '*': {
                'format': '%Y-%m-%d %H:%M:%S',
                'timezone': 'GMT',
            }

        },
        '4': {
            '*': {
                'format': '%Y-%m-%d',
                'timezone': 'GMT',
            }
        },
        '5': {
            '*': {
                'format': '%Y-%m-%d',
                'timezone': 'GMT',
            }
        }
    }
)
```


***Application::create_data***: 

This is when you create your genuine reactions, ETLs and have new . persistent data (datasets, models, etc). An output of this API is called as ‘*internal data*’ in RheocerOS. You basically describe which input signals should be joined on what conditions and trigger what type of inlined, batch, model or failure_handler compute executions.

_Note_: Once you add new data nodes into your application, then they are available as inputs or test-signals to the rest of your application code or downstream applications that import your application, pretty much the same way other nodes (created by ‘marshal_external_data’ or ‘create_timer’) are used.


Synopsis:

```
    def create_data(self,
                id: str,
                inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]],
                input_dim_links: Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]],
                output_dimension_spec: Union[Dict[str, Any], DimensionSpec],
                output_dim_links: Sequence[Tuple[str, DimensionVariantMapFunc, SignalDimensionTuple]],
                compute_targets: Sequence[InternalDataNode.ComputeDescriptor],
                **kwargs) -> MarshalerNode:
```


**Params**:

_id_ : internally unique ID of the dataset. you might use this *id* later to refer to this node in cases where you won’t have the returned value (<MarshalerNode>) in the context. Conflicts around these identifiers will raise validation errors from the framework. “id”s are mostly used when you import nodes from other applications. Please refer [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND) for more details on how “id” field is used. Exceptionally for ‘the nodes created by ’creat_data’ API, id is more important for route management and that is why these nodes are more likely to be accessed/referred via their id fields in practice.

_inputs_:  Use returned values from upstream API calls here. Relies on [Dimension Filter Chaining: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CAOGJy2). This is also called ‘input binding’. Either a list or a dictionary of input aliases and their [Filtered View: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CA8GgEu) s created by implicit dimension filter chaining. If you don’t want to bother yourself with extra filtering (*FilteredViews)* against your inputs, then leave them as is (as *[MarshalerNode](https://quip-amazon.com/hCN1A8Z4uVHi/RheocerOS-Programming-Suite#LHQ9CAE9kJH)*s). It will be equivalent to an ‘all-pass’ filter. Inputs (using the alias’ and runtime dimension values from incoming signals) will be passed into user “[compute_targets: an array of user…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAaRfSh)” as Spark data-frames automatically, depending on the ABI chosen as part of the code declaration. 

_input_dim_links_: A list of DimensionLink ([Dimension Link: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CAD2qTM)) somewhat represents the dimension mapping logic between all of the inputs. RheocerOS uses this information to understand trigger groups / links at runtime. When multiple signals that belong to your inputs received and checked against your new node, RheocerOS uses this list (as a matrix) to extract consistent event groups and use them to initiate a trigger, or say execution. Ex: group events (that were received for my inputs) together if they all have the same date partition value. In order to achieve this logical join, you should help RheocerOS to resolve mapping between equivalent dimensions from different inputs. If they are actually not equivalent out of the box, you should provide the Python Lambda code to map them. Yes, you are right, quite similar to SQL JOIN operations.
Example: for a matrix 
[ 
   (input_signal1[‘region_id’], lambda x: {‘NA’: 1, ‘EU’: 2}[x], input_signal2[‘region’]),
   (input_signal2[‘order_day’], lambad day: day.strftime(‘%d-%m-%Y’), input_signal1[‘ship_day’])
]
is equivalent to the following Python code:
  
input_signal1.region_id = {‘NA’: 1, ‘EU’: 2}[input_signal2.region]
input_signal2.order_day = input_signal1.ship_day.strftime(‘%d-%m-%Y’)

So at runtime, only an event group like the following will be considered for the same execution (trigger) context:

input_signal1 → (‘1’, ‘2020-05-01’)
input_signal2 → (‘NA’, ‘01-05-2020’)

If these two are received by your node (assuming that you have no other input dependency), then you’ll have an execution context created and all of your [compute_targets: an array of user…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAaRfSh) will be triggered (except the failure_targets depending on the collective, ultimate status of the other targets).

_output_dimension_spec_: Same as [dimension_spec: a hierarchical description of…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAmVUOd) from *Application::marshal_external_data* above. For example, if you are about to create a new dataset (depending on your ‘*compute_targets’ param below),* this simply outlines the partitioning scheme for a dataset.

_output_dim_links_: Similar to [input_dim_links: A list of DimensionLink…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAbWxdD) but this time for the final dimension value assignment to the output at runtime. This map determines the ultimate values of dimensions for the output, mapping them from the runtime dimensions of the inputs. It is available to user’s Spark code at runtime as ‘***dimensions***’. Please see [this example](https://code.amazon.com/packages/RheocerOSBlade/blobs/14d2304591ff94486484c57ae8327fcddba4f6a3/--/src/intelliflow_blade/data/feature_engineering/import/d_ship_option.py#L44) on how you can read the runtime values of the dimensions and use them in your script (if you have to). In majority of the cases, utilizatoin of ‘dimensions’ context variable is not necessary since your input dataframes are created with the necessary filtering / parametrization. 

_compute_targets_: an array of user provided compute codes wrapped by specific ComputeDescriptor impls (InlinedCompute, BatchCompute/GlueBatchCompute, ModelCompute, BatchModelCompute, etc). Depending on the ABI (Application Binary Interface or call convention), RheocerOS auto-generates the prologue/epilogue for user “code” (either as a string or a Python module). This makes it possible even for a Cradle script to be executed in RheocerOS’s own batch compute drivers irrespective of the underlying compute resources. Similar considerations apply to the choice of language and Spark versions. One important detail to emphasize here is that you can make a **hybrid** use of languages (Python, Scala) when you are declaring your batch-compute (this will be covered in the Use Cases section). So your high-level application code will be in Python, but you’ll have the flexibility to use this as a glue-layer to bind your business logic from Scala, Python packages from your version set.

For an execution to be marked as successful, all of the compute targets should succeed. They all get executed as soon as the trigger (event) group is detected. ‘Pre’ or ‘Post’ processing features will be evaluated in the future.

_*Inlined compute targets*_ give you a Python execution environment in AWS (e.g Lambda inlined with boto3.Session and other necessary primitives ready) with the runtime state of your inputs and the physical path of your output, and some other data. Inputs and output are presented in [Signal: RheocerOS Programming Suite](https://quip-amazon.com/hCN1A8Z4uVHi#LHQ9CABS3ER) format which is an immutabe plain Pyhon object. You can also use your own dependencies, Python modules here. Failure criteria is an ‘Exception’, when RheocerOS would mark this compute attemp as a failure (with the dump of the stack trace).

Synopsis:
*InlinedCompute*( *Callable*[ *Dict*[*str*, [Signal](https://code.amazon.com/packages/RheocerOS/blobs/4310e099befdb8541a9d824d0325a2fbf9bc0640/--/src/intelliflow/core/signal_processing/signal.py#L98)], [Signal](https://code.amazon.com/packages/RheocerOS/blobs/4310e099befdb8541a9d824d0325a2fbf9bc0640/--/src/intelliflow/core/signal_processing/signal.py#L98), *Dict*[*str*, *Any*] ])

so basically you are expected to provide a Python callable (function, lambda) with the signature represented by (within) the Callable type above. See the example below;

```

          # this function is to be registered to 'create_data' call below 
          # as a compute callback
          def example_inline_compute_code(input_map, output, params):
                """ Function to be called within RheocerOS core.
                Below parameters and also inputs as local variables in both
                    - alias form ('order_completed_count_amt')
                    - indexed form  (input0, input1, ...)
                will be available within the context.
            
                input_map: [alias -> input signal]
                output: output signal
                params: contains platform parameters (such as 'AWS_BOTO_SESSION' if an AWS based configuration is being used)

                Ex:
                s3 = params['AWS_BOTO_SESSION'].resource('s3')
                """
                print("Hello from AWS!")
                
                offline_data_signal: Signal = input_map['offline_data']
                
                incoming_event_resouce_paths: List[str] = offline_data_signal.get_materialized_resource_paths()
                
                # extract bucket/keys from this full path
                output_partition_full_resource_path: str = output.get_materialized_resource_paths()[0]                
                # assuming that you added Pandas to your application package as a dependency already
                import pandas as pd
                `import boto3import io
                s3 = boto3.client('s3')
                df = pd.read_csv(output_partition_full_resource_path)
                ...`      
                s3.put_object(Bucket='my_random_bucket_for_dataframe_analysis', Key='...', ...)              
                
                
                
          training_features_1 = app.create_data('order_completed_count_amt', # data_id
                                 # {alias -> filtered input map}
                                 {
                                     "offline_data": eureka_offline_data["*"]["*"],
                                 },
                                 [],  # dim link matrix for inputs
                                 {
                                        'region': {
                                            'type': DimensionType.STRING,
                                            'day': {
                                                'type': DimensionType.DATETIME,
                                                'format': '%Y-%m-%d',
                                            }
                                        }
                                 },
                                 [
                                     ('region', lambda dim: dim, eureka_offline_data('region')),
                                     ('day', lambda dim: dim, eureka_offline_data('day'))
                                 ],
                                 [ # when inputs are ready, trigger the following
                                      InlinedCompute(
                                          #lambda inputs, output: print("Hello World from AWS")
                                          example_inline_compute_code
                                      ),
                                      BatchCompute(
                                          "output = offline_data.withColumn('new_col', lit('hello'))",
                                          Lang.PYTHON,
                                          ABI.GLUE_EMBEDDED,
                                          # extra_params
                                          # interpreted by the underlying Compute (Glue)
                                          # and also passed in as job run args (irrespective of compute).
                                          spark_ver="2.4",
                                          # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                                          MaxCapacity=5,
                                          Timeout=30 # in minutes
                                      )
                                 ]
                                 )
          
```


*_Batch-compute targets_* give you a remote PySpark/Scala/... based execution environment with different ABI support. Success/failure criteria depends on the underlying driver (Glue, etc):

Python + Spark:
 GLUE ETL (with no dependency support): your spark code should either be a string or a separate Python module with no extra dependencies.


Scala + Spark (**BETA**):
GLUE ETL (with no dependency support): your spark code should be a string.
GLUE ETL (with your project / VS bundle): similar to PySpark ETL case with one di
CRADLE_ABI (with your project / VS dependencies): convenience support to easily migrate business logic from Cradle based pipelines.


Synopsis:

```
                [BatchCompute](https://code.amazon.com/packages/RheocerOS/blobs/76c2716688299e85446c1ccf3e98ab403d23d635/--/src/intelliflow/core/application/context/node/internal/nodes.py#L50)(code: str,
                            lang: [Lang](https://code.amazon.com/packages/RheocerOS/blobs/551e71a24447a71e1d70c05c9c131e34e29de7cd/--/src/intelliflow/core/signal_processing/definitions/compute_defs.py#L5),  # PYTHON | SCALA
                            abi: [ABI](https://code.amazon.com/packages/RheocerOS/blobs/551e71a24447a71e1d70c05c9c131e34e29de7cd/--/src/intelliflow/core/signal_processing/definitions/compute_defs.py#L20),    # GLUE_EMBEDDED | CRADLE | ...
                            # driver specific parametrization
                            # if you are default Glue based batch-compute,
                            # then see (irrespective of 'Lang' param above)
                            # *https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run*
 ** **kwargs     
                            )
```



_*Model targets*_ (**BETA**):

Sagemaker Training Job: bind input alias’ as test or validation and let RheocerOS do the binding at runtime. Then provide other training job specific params.

Sagemaker Batch-Transform: 

_Failure handler targets_ (**v1.0**)**:** Mark any of the above to be executed on failed execution. Execution context is similarly passed into the compute target. Ex: use a custom inlined compute target code to handle error in an AWS context, get CW, etc from boto3 and take care of metrics, persist the result, trigger an error analytics NAWS pipeline (outside of RheocerOS).

_kwargs_: user-defined metadata. It is important for your own future reference and more importantly when other users import your application code into theirs. This will be exposed to them via ‘access_spec()’ call on your node (more formally *as a part of the return value of MarshalerNode::access_spec* ).

**Returns**: bind the returned variable to downstream API calls such as [Application::create_data:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAZMmq5) or Application::materialize.

_[MarshalerNode](https://quip-amazon.com/hCN1A8Z4uVHi/RheocerOS-Programming-Suite#LHQ9CAE9kJH)_: Almost all of the RheocerOS data manipulation APIs will use this type as the return type. Users generally (and implicitly) use the APIs of this type for extra operations, local analysis and filtering on the dataset. 

When binding datasets (and other data) into other APIs, RheocerOS expects “marshaled” types like this as the input. For dataset APIs, MarshalerNode is the input-output format, binding factor across a chain of operations. 

Example 1 (use the output ‘eureka_offline_all_data’ from [eureka_offline_all_data = app.marshal_external_data( external_data_desc= S3Dataset("427809481713",…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAGEWjG)):

```
default_selection_features = app.create_data(**'eureka_default_selection_data_over_two_days'**,  # data_id
 ** {
                                          **"offline_data"**: eureka_offline_all_data[:-2],
                                          **"offline_training_data"**: eureka_offline_training_data[**"NA"**][:-2]
                                      },
                                      [
                                          (eureka_offline_all_data(**'day'**), lambda dim: dim, eureka_offline_training_data(**'my_day_my_way'**))
                                      ],  
                                         *# dim link matrix for inputs* {
                                          **'reg'**: {
                                              **'type'**: DimensionType.STRING,
                                              **'day'**: {
                                                  **'type'**: DimensionType.DATETIME,
                                                  **'format'**: **'%d-%m-%Y'**,
                                              }
                                          }
                                      },
                                      [
                                         (**'reg'**, lambda dim: dim, eureka_offline_training_data(**'region'**)),
                                         # we will use a reversed format for the 'day' partition of this new data
                                         (**'day'**, lambda dim: dim.strftime(**'%d-%m-%Y'**), eureka_offline_all_data(**'day'**))
                                      ],
                                      [ *# when inputs are ready, trigger the following* 
 ** BatchCompute(
                                              **"output = offline_data.subtract(offline_training_data)"**,
                                              Lang.PYTHON,
                                              ABI.GLUE_EMBEDDED,
                                              *# extra_params*
 ** *# interpreted by the underlying Compute (Glue)*
 ** *# and also passed in as job run args (irrespective of compute).*
 ** spark_ver=**"2.4"**,
                                              *# see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run*
 ** MaxCapacity=5,
                                              Timeout=30 *# in minutes*
 ** )
                                      ]
                                      )
```



Example 2: Transform a table from Andes (use the output ‘ducsi_data’ from [ducsi_data = app.marshal_external_data( AndesDataset("booker", "d_unified_cust_shipment_items",…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CA9Fag5))


```
repeat_ducsi = app.create_data(**"REPEAT_DUCSI"**,
                               {
                                   # use the last three days from no matter what region
                                   # the incoming signal is from.
                                   **"DEXML_DUCSI"**: ducsi_data[**"*"**][:-3]
                               },
                               [],
                               {
                                   # we could actually use different names here
                                   **'region_id'**: {
                                       **'type'**: DimensionType.LONG,
                                       **'ship_day'**: {
                                           **'type'**: DimensionType.DATETIME
                                       }
                                   }
                               },
                               [
                                    # my dimensions are identical to my input's
                                   (**'region_id'**, lambda dim: dim, ducsi_data(**'region_id'**)),
                                   (**'ship_day'**, lambda dim: dim, ducsi_data(**'ship_day'**))

                               ],
                               [
                                   # this very simple spark code will use IF provided
                                   # data-frame DEXML_DUCSI and set it to the special
                                   # variable output. This is an example of string based
                                   # PySpark code. 
                                   GlueBatchCompute(**"output=DEXML_DUCSI"**)
                               ]
                               )
```

### Timers

***Application::create_timer***: This simple API allows you to create your own schedule-based Signals and bind them to [Application::create_data:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAZMmq5) calls. For an AWSApplication, RheocerOS leverages AWS EventBridge to emulate this logical signals and cause triggers in your downstream nodes. Provided abstraction around EventBridge is very thin (just like every other that RheocerOS introduces) and relies on natively supported CROD or 

Synopsis:

```
    def create_timer(self,
                    id: str,
                    schedule_expression: str,
                    date_dimension: DimensionVariant,
                    **kwargs) -> MarshalerNode:
```

**Params**:

_id_: similar to the ‘id’ param of other APIs, this field of a Timer node will allow you to find it in contexts where the returned node variable is not available.
_schedule_expression_: https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
_date_dimension_: Timer signals have only one dimension (of DateTime format). You can only specify the name of it for your own convenience, when you are using this downstream in downstream ‘create_data’ calls (particularly while referring it for [input_dim_links: A list of DimensionLink…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAbWxdD)).

Example 1: Run/Trigger the following node every month. 

* A new ‘dates’ partition (internal dataset partition) will be created upon a successful execution of this code. The [Python module](https://code.amazon.com/packages/RheocerOSBlade/blobs/mainline/--/src/intelliflow_blade/data/feature_engineering/import/dates.py) being used here is creating a look-up table based on the (new) dynamic value of ‘cutoff_date’ dimension of the Timer signal.
* A new ‘d_ship_option’ (internal dataset partition) will also be created simultaneously. The [Python module](https://code.amazon.com/packages/RheocerOSBlade/blobs/mainline/--/src/intelliflow_blade/data/feature_engineering/import/d_ship_option.py) being used in the second ‘create_data’ call is implicitly rely on a table from Andes (assuming that [Andes BDT-Glue sync](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CADPJzx) is done and the DEXBI:D_SHIP_OPTION table is subscribed to already). So it reads the whatever latest version of that table is ready at the beginning of each month and creates a partition for it (like ‘1/1/2020-11-01’).



```

region_id = 1
marketplace_id = 1

# MONTHLY TIMER SIGNAL
monthly_timer_signal = app.add_timer("blade_monthly_timer"
                                     ,"cron(0 0 1 * ? *)" # Run at 00:00 am (UTC) every 1st day of the month
                                     ,"cutoff_date"
                                     )
                                     
dates = app.create_data('dates',
                         # input list (no alias')
                         [
                            # equivalent to monthly_timer_signal['*']
                             monthly_timer_signal
                         ],
                         # dim link matrix for inputs
                         [],
                         BLADE_INTERNAL_DIMENSION_SPEC
                         ,
                         [
                             ('region_id', lambda dim: dim, region_id),
                             ('marketplace_id', lambda dim: dim, marketplace_id),
                             ('date', lambda dim: dim, monthly_timer_signal('cutoff_date'))
                         ],
                         [ # when inputs are ready, trigger the following
                            GlueBatchCompute(
                                # the following module should exist in Python path
                                # or in the 'src' folder of the host package (that
                                # contains this app script as well).
                                python_module("intelliflow_blade.data.feature_engineering.import.dates"),
                                MaxCapacity=5,
                                Timeout=4 * 60  # 4 hours
                            )
                         ]
                         )
                         
                         
d_ship_option = app.create_data('d_ship_option',
                        [
                            monthly_timer_signal['*']
                        ],
                        [],
                        BLADE_INTERNAL_DIMENSION_SPEC
                        ,
                        [
                            ('region_id', lambda dim: dim, region_id),
                            ('marketplace_id', lambda dim: dim, marketplace_id),
                            ('cutoff_date', lambda dim: dim, monthly_timer_signal('cutoff_date'))
                        ],
                        [ # when inputs are ready, trigger the following
                            BladeGlueBatchCompute("feature_engineering.import.d_ship_option",
                                                  MaxCapacity=5,
                                                  Timeout=4 * 60  # 4 hours
                                                  )
                        ]
                        )
```



## Application Activation

At any point during the development of your Application, you can call *Application::activate*;


```
app.activate()
```

This call intiates a provisioning/update sequence against the target AWS platform (account + region + app_id). This is how your remote AWSApplication is created or updated based on the topology of your nodes.

The structure, architecture of the auto-generated NAWS application does not change much based on your business logic, however the parametrization, internal data, sub (AWS) resources, (IAM) permissions, external/internal notification channels (S3, SNS, etc) of the main components might change. 

But as a user, you are generally agnostic from those details. You are expected to care about the logical routing structure, and for that, the IDs of your nodes. Your nodes map to _route_ entries in the final application (encapsulated by RoutingTable). And subsequent event consumption, executions/triggers are managed per route (hence node) at runtime. 

You can just rely on this basic understanding to start using the functionalities such as route/execution management, etc

[Image: image.png]

Once successfully activated, this generic application runtime/engine runs in your target account and region in a sandboxed way. This application state is independent from your local Python environment (dev-box, notebook, etc) and it is what you are interested in while taking any actions (introduced in the subsequent sections), in the same env or others for the same app.


## Synthetic Events / Manual Triggers

Using any Signal reference from upstream API calls, raw events (in dict format [CW, S3, SNS, ...]) or via [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND) from the application objects  you can emulate runtime situations by injecting synthetic events into the system following [Application Activation](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAkUrgT) (otherwise, the following APIs are no-op).

This will enable you to cause triggers on the entire topology or one branch or a specific node of it. You basically know what type of input dependencies your nodes have.

For a basic scenario of triggering an execution on a node, you just need to manually feed events for each one of its inputs. Those events should match in dimenion values (partitions), according to the link matrix you provide as part of  [input_dim_links: A list of DimensionLink…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAbWxdD) . Otherwise, they will still be consumed by the system and only cause pending execution contexts remotely.

Common use-cases are:

*  testing after the introduction of a ‘new node (via ’create_data’).
* programmatic backfilling of an entire pipeline or a branch of it (depending on which node[s] is meant to catch them). Since RheocerOS uses event-based triggering, you usually just need to provide the initial (event) seeds and then the subsequent executions in downstream nodes happen automatically thanks to event propagation (upon successful completions).
* Re-runs (manual job executions in conventional terminology).


***Application::process:* **send in raw events or use data references or their filtered versions to feed the system.

Synopsis:


```
       def process(self,
                input: Union[Signal,
                             Dict[str, Any],
                             FilteredView,
                             MarshalerNode],
                with_activated_processor=False,
                processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD) -> None:
```


**Params**:

_input_:  Event to be processed. Supports four different types for outmost convenience.

Example for each format:

<Signal> type:
 

```
        input_data = app.create_data(...)
      
        node_to_be_triggered = app.create_data(inputs=[input_data], ...)
      
        app.process(input_data[dim_val_1]....[dim_val_N])
```


Dict[str, Any] raw type (aka raw type): use when you have an idea about what it is going on at the low level. Mostly used by ‘RheocerOS Core Framework’ developers when a new event-channel is being introduced to the framework. But it is also available to clients for convenience particularly when they are adding external data (S3, Andes, etc) to their application.

```

        # feed raw S3 notification into the system.
        # if an external data (that matches the details here) is already used as an
        # input in any of the create_data calls, then this could possibly contribute to
        # a trigger.
        app.process({
    "Records":[
        {
            "eventVersion":"2.0",
            "eventSource":"aws:s3",
            "awsRegion":"us-east-1",
            "eventTime":"1970-01-01T00:00:00.000Z",
            "eventName":"ObjectCreated:Put",
            "s3":{
                "s3SchemaVersion":"1.0",
                "bucket":{
                    "name":"dex-ml-eureka-model-training-data",
                    "arn":"arn:aws:s3:::dex-ml-eureka-model-training-data"
                },
                "object":{
                    "key":"cradle_eureka_p3/v8_00/all-data/partition_day=2020-03-18/_SUCCESS",
                }
            }
        }
    ]
}
```


_with_activated_processor (advanced use)_: this boolean flag determines whether the call will use local Processor code (SYNC) or remote (AWS Lambda, Fargate, etc) Processor endpoint to interpret and route the event. It is False default, meaning that local Processor is used by default which enables the user to debug the internals of RheocerOS and possible runtime behaviour without having to wait for a real event to be received at runtime (which currently can be debugged/analyzed using routing APIs or directly going to the CW logs of the Processor impl [e.g AWS Lambda]).

_processing_mode (advanced use)_: when a Signal or a raw event represents multiple physical resources, this flag determines which one of them will be routed into the system. Default mode chooses the first one. Other mode ‘FULL_RANGE’, causes the injection of all of the events into the system.

Example:

For a simple application like the following, where we have an external S3 data and then an internal data that does some transformation on it, we can manually trigger the execution on the internal-data (output of the ‘create_data’ call) by using *Application::process* API.

(1) In the same application code (check the lines before the ‘activate’ call in the end):

```
app = AWSApplication(...)

eureka_offline_all_data = app.marshal_external_data(
                                    external_data_desc= S3Dataset(...)
                                    ...)
                                    
# 'add_external_data' convenience API provided by 'intelliflow.api_ext' module.
# a simpler (yet less powerful) wrapper around 'marshal_external_data'.
eureka_offline_training_data = app.add_external_data(
                                            **"eureka_training_data"**,
                                            S3(**"427809481713"**,
                                               **"dex-ml-eureka-model-training-data"**,
                                               **"cradle_eureka_p3/v8_00/training-data"**,
                                               StringVariant(**'NA'**, **'region'**),
                                               AnyDate(**'my_day_my_way'**, {**'format'**: **'%Y-%m-%d'**})))

default_selection_features = app.create_data(
                                     data_id = 'eureka_default_selection_data_over_two_days',
                                     inputs=[
                                          eureka_offline_all_data[:-2],
                                          eureka_offline_training_data["NA"][:-2]
                                     },
                                     input_dim_links=[                     [
                                          (eureka_offline_all_data('day'), lambda dim: dim, eureka_offline_training_data('my_day_my_way'))
                                     ],  
                                     ...
                                     ) 
app.activate()

# satisfy 1st input
app.process(eureka_offline_all_data[**'2020-03-18'**])

# satisfy 2nd input
# please note that same value is chosen on the second dimension
# this is due to how those inputs are linked to each other.
# they are equivalent due to the 'input_dim_links' declaration in the 'create_data'
# call above.
# if the input_dim_links was something like;
#    (eureka_offline_all_data('day'), lambda dim: dim.strftime('%Y-%m'), eureka_offline_training_data('my_day_my_way'))
# 
# then we would have to use the following (without day) for a trigger to occur;
#  app.process(eureka_offline_training_data['NA']['2020-03'])
app.process(eureka_offline_training_data[**'NA'**][**'2020-03-18'**])

# EXECUTION ON NODE 'eureka_default_selection_data_over_two_days' STARTS REMOTELY!

# now at this point, you can see Glue Job execution for route 'eureka_default_selection_data_over_two_days'
# or use ['Route/Execution Monitoring' APIs](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CAQwSZu) to find the route and then this new 'active' execution.

# remote execution (e.g big-data job) and the pyshical path, partitioning for the output
# will use 'NA', '2020-03-18' automatically based on the dimension_spec declaration
# in the 'create_data' call and also the common dimension values used in the 'process'
# calls above.

                                   
```

Ignored (NOOP) event:


```
 app.process(eureka_offline_training_data['EU']['2020-03-17'])
```

This line won’t cause anything since ‘create_data’ call explicitly uses a band-pass filter to specify ‘NA’ region on the external data ‘eureka_offline_training_data’.

(2) Outside of Application code: When you just know the application related info but don’t have the code. You have to get the references first (see [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND) for more details).


```
app = AWSApplication(...)

input1 = app['eureka_training_all_data'] # 'id' used in the marshal_external_data call
input2 = app['eureka_training_data']

# satisfy 1st input
app.process(input1['2020-03-18'])
app.process(input2['NA']['2020-03-18'])

# EXECUTION!
```




## Application Analysis / Route-Execution Monitoring

### Finding Nodes

Experience on retrieving the references for your nodes (for inputting, event triggering, etc) changes based on whether your application is activated or not (see [Application Activation](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAkUrgT)).

***Application::list_data:***  This API will return an iterable sequence of data nodes contained by an application and all of its dependencies _by default_ (other applications imported into its context, please see [Connecting with other Applications](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAM0RqM) for more details on that).

Synopsis:

```
          def list_data(self,
                  app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
                  context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT
                  ) -> Sequence[MarshalerNode]:
```


**Params**: 

_app_scope_: parameter that controls the search space among the connections of the application. Enumeration values are as follows:

*QueryApplicationScope.ALL*: default behavior, finds all of the data_nodes spanning the whole dependency network of the application.

*QueryApplicationScope.CURRENT_APP_ONLY*: limits the search space to the current application, if you are interested in finding your own nodes (the ones that your introduced via ‘marshal_external_data’ or ‘create_data’ calls).

*QueryApplicationScope.EXTERNAL_APPS_ONLY*: limits the search space to the dependencies of the application, excluding your own nodes (the ones that your introduced via ‘marshal_external_data’ or ‘create_data’ calls).

_context_: controls the search space based on whether you are interested in the already activated state of the application or not. All of your application nodes and dependencies are maintained in two different contexts. 

*QueryContext.ACTIVE_RUNTIME_CONTEXT*: default behaviour, use only the nodes and dependencies determined by the previous activation of this application. So if the application is not activated yet and you keep this default value, then this API and most of other retrieval APIs will give empty results.

*QueryContext.DEV_CONTEXT*: even for an application which was not activated yet, this value would allow you to retrieve upstream node references programmatically (during the development). You rarely want to use this parameter since in majority of the cases it would not make a lot of sense to retrieve your nodes while they are already available in the curernt Python development context as variables. So if you don’t want to pass around the return values (node references) of the upstream data API calls but actually retrieve them from the current development state of the application object, then you can resort to this parameter.

**Returns**:
*Sequence[MarshalerNode]* :  each reference can be directly hosed into downstream API calls such as create_data (with or without extra filtering). Check ‘inputs’ and ‘input_dim_links’ parameters from the [Params:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAt7sOi) section of ‘create_data’ API call to understand that. You might want to use ‘MarshalerNode::access_spec’ and ‘MarshalerNode:dimension_spec’ methods to get extra information on these nodes.

**Examples**: 

Example 1: default utilization 

```
      my_app = AWSApplication(id, region, acc)
      
      # if the application was not activated already (no topology in AWS yet)
      # then this call will return an empty list.
      datasets_or_models = my_app.list_data()
      
      for data in datasets_or_models:
        pprint(data.access_spec())     # print full access (storage, resource path) details and metadata
        pprint(data.dimension_spec())  # dump current dimension domain and default filtering rules.
        
```


Example 2: other use-cases; please check the comments inlined below.

Assume that your application was activated from your dev-box with a topology as follows:

```
      
      my_app = AWSApplication(id, region)
      
      # two upstream applications which have multiple data-nodes that we don't own
      # but can import and use seamlessly. 
      upstream_app_1 = my_app.import_upstream_application(upstream_id_1, aws_acc, aws_region)
      
      upstream_app_2 = my_app.import_upstream_application(upstream_id_1, aws_acc_2, aws_region_2)
      
      foo_ref = my_app.create_data(data_id='foo', ...)
      
      bar_ref = my_app.create_data(data_id='bar', inputs =[foo_ref], ...)
      
      # before the activation try to get the handle all of the nodes created so far
      # owned by 'my_app' only
      my_nodes_so_far = set(my_app.list_data(QueryApplicationScope.CURRENT_APP_ONLY,
                                                           QueryContext.DEV_CONTEXT))
      
      assert({foo_ref, bar_ref} == my_nodes_so_far, 'you should not see this message!')
      
      # since the app is not activated yet, upstream applications are also in
      # the dev context still. that is the reason for the second param.
      visible_upstream_nodes = set(my_app.list_data(QueryApplicationScope.EXTERNAL_APPS_ONLY,
                                                    QueryContext.DEV_CONTEXT))
      
      assert(set(upstream_app_1.list_data()).union(set(upstream_app_2.list_data())) \
             ==
             visible_upstream_nodes,
             'you should not see this!')
      
      my_app.activate()
      
```


And now assume that you want to analyze your application form another end-point (e,g notebook):
 

```
     my_app = AWSApplication(id, region)
     
     # this will list you all data visible to 'my_app': 
     #  its own nodes + nodes from upstream dependencies
     all_datasets_or_models = my_app.list_data()
     
     assert(all(id in {node.access_spec()['id'] for node in all_datasets_or_models}\
                for id in {'foo', 'bar'}),
            'you should not see this!')
            
           
     # following call should give you nothing. 
     # since 'my_app' instance has nothing in local dev_context so far.
     # it can only return answers for the remote active state.
     my_dev_nodes_so_far = set(my_app.list_data(QueryApplicationScope.ALL,
                                                QueryContext.DEV_CONTEXT))
                                                
     assert(not my_dev_nodes_so_far, 'RheocerOS internal error')
     
     # BUT if we do attach and pull active state/topology into local dev_context;
     my_app.attach()
     
     # then your dev and active contexts would be identical (till your next create_data
     # call) and list_data or other retrieval APIs would give same results
     # no matter what you choose for the 'context' variable.
     my_dev_nodes_so_far = set(my_app.list_data(QueryApplicationScope.ALL,
                                                QueryContext.DEV_CONTEXT))
                                                
     assert(all_datasets_or_models == my_dev_nodes_so_far, 'RheocerOS internal error')
     
```



***Application::get_data:* **

Similar to [Application::list_data](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CAhyZu1) returning a (indexable) list of data nodes which have their IDs equal to the input parameter ‘data_id’.

Synopsis:

```
          def get_data(self,
                 data_id: str,
                 app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
                 context : QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT) -> List[MarshalerNode]:
```


**Params**:

_data_id_: search operation relies on exact (str) matches with this param and returns the result accordingly. Normally, within an application duplicate IDs are not allowed, so you might wonder why the return type is of list type. The answer is due to ‘upstream’ dependencies which have data matching the IDs of your internal data (IDs that you chose as part of ‘create_data’ or ‘create_timer’ calls).

_app_scope_: Please refer [app_scope: parameter that controls the…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAddQFX) from *list_data* API.

_context_: Please refer [context: controls the search space…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAttQLx) from *list_data* API.

**Returns**:
*List[MarshalerNode]* :  each  (indexable) reference can be directly hosed into downstream API calls such as create_data (with or without extra filtering). Check ‘inputs’ and ‘input_dim_links’ parameters from the [Params:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAt7sOi) section of ‘create_data’ API call to understand that. You might want to use ‘MarshalerNode::access_spec’ and ‘MarshalerNode:dimension_spec’ methods to get extra information on these nodes.

Examples: please refer [Examples:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CA12ALP) section of list_data API for advanced operations on ‘app_scope’ and ‘context’ parameters which pretty much apply to this API as well.


```
      # assume that this application was activated before with a 'create_data' call
      # using the data_id 'filtered_D_ADDRESSES'
      my_app = AWSApplication(id, region)
      
      d_addresses = my_app.get_data('filtered_D_ADDRESSSES')[0]
      
      # now use d_addressees as an input or as an event trigger.
```


***Application::query_data***:  Quite similar to ‘[Application::get_data:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAh8HSt)’. Rather than an exact match, it makes a different use of the input and performs a ‘contains’ check against the ‘id’ fields of all of the target nodes within the context of the application. And also, its return type is different and it is a map of ‘id → node’ pairs.

Synopsis:

```
          def query_data(self,
                   query: str,
                   app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
                   context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT
                   ) -> Mapping[str, MarshalerNode]:
```


**Params**:

_query_: used to find all of the nodes that fall to the target group determined by ‘app_scope’ and ‘context’ and also contain this query string.

_app_scope_: Please refer [app_scope: parameter that controls the…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAddQFX) from *list_data* API.

_context_: Please refer [context: controls the search space…](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAttQLx) from *list_data* API.


Examples:

```
      datasets_that_contain_shipment_in_IDs = app.query('shipment')
      
      pprint(datasets_that_contain_shipment_in_IDs)
      
      ducsi = datasets_that_contain_shipment_in_IDs['d_unified_cust_shipment_items']
      
      # trigger all of the branches that has this data as the common ancestor
      app.process(ducsi['NA']['2020-06-21'])
            
```

***Application::search_data***: Similar to Application::query_data but does the search on entire node information + metadata attached to it.

***Application::get_timer***: focuses the search around [Timers](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAUXRC2). Please refer [‘get_data’ API](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CAh8HSt) for parametrization and general utilization.

Synopsis:

```
             def get_timer(self,
                        timer_id: str,
                        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
                        context : QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT) -> List[MarshalerNode]:
```


***ApplicationExt::__getitem__:* **commonly used** ** **convenience API (from ‘intelliflow/api_ext’ module) which is a restricted version of ‘[get_data’](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CAh8HSt)and ’get_timer’. APIs. This tries to find an exact match for the entity_id and raises an exception if it cannot find any. Target entity can be of both types data or timer. For more information on different entity types, please refer [Application Update / New Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAG0fEs).

! _Warning_ !: this convenience API uses QueryApplicationScope.ALL and ‘QueryContext.ACTIVE_RUNTIME_CONTEXT’ internally. For more details on the impact of those search criteria, please refer the [Params:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAYIfZE) section of ‘*list_data*’ API.

Synopsis:

```
          def __getitem__(self, entity_id) -> MarshalerNode:
```

**Params:**

_entity_id_: id of the target entity to be found. It should match the ‘data_id’ or ‘timer_id’ params for the ‘create_data’ or ‘create_timer’ calls, respectively.

**Returns**:
*MarshalerNode* : this can be directly hosed into downstream API calls such as create_data (with or without extra filtering). Check ‘inputs’ and ‘input_dim_links’ parameters from the [Params:](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAt7sOi) section of ‘create_data’ API call to understand that. You might want to use ‘MarshalerNode::access_spec’ and ‘MarshalerNode:dimension_spec’ methods to get extra information on these nodes.

Examples:


```
      my_app = AWSApplication(id, region, acc)
      
      customers_data = my_app['filtered_customers_data']
      
```



### Route/Execution Monitoring

At runtime (after  [Application Activation](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAkUrgT)), your nodes (i.e created by ‘create_data’ call), registered as ‘routing’ decision entries into the *RoutingTable.* Incoming signals are evaluated by these entries, whether you’ve triggered them via [Synthetic Events / Manual Triggers](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAWhCEO) or some event has been received by your remote application in the background (i.e on the cloud side) asynchronously. And of course, the actual execution happens remotely. 

So, APIs within this group help you to track/monitor the status of ongoing/active or historical executions on your routes (nodes).


```
def get_routing_table(self) -> Iterable[Route]:
```



```
def get_active_routes(self) -> List[RoutingTable.RouteRecord]:
```



```
def get_active_route(self, route: Union[str, MarshalerNode, Route]) -> RoutingTable.RouteRecord:
```



```
def update_active_routes_status(self) -> None:
```



```
def update_active_route_status(self, route: Union[str, MarshalerNode, Route]) -> None:
```



```
def get_inactive_compute_records(self, route: Union[str, MarshalerNode, Route], ascending: bool=True) -> Iterator[RoutingTable.ComputeRecord]:
```



```
def get_active_compute_records(self, route: Union[str, MarshalerNode, Route]) -> Iterator[RoutingTable.ComputeRecord]:
```



### Data Analysis

APIs here provide the ability to peek into the data or load some or all parts of it. These unified scheme allows user to check the content of an external or internal data partition referred by material/concrete dimension values.

***Application::preview_data***: Previews the materialized input node by accessing the remote resource. Previewed portion of the remote data can be controlled by the user by ‘limit’ and ‘column’ parameters.

This API is opportunistic and fall backs on to different strategies for the most efficient/convenient way loading the data. Based on the runtime configuration of the current (Python) process context, it uses the following order to load the data:

*Pandas [Dataframe] → Spark [Dataframe] → Python CSVReader (finally rendered as [Python List])*

While loading the data implicitly, it also dumps the preview result to the console and (if on Jupyter) to ‘display’ (for beatiful rendering). So the returned Tuple of data and format can be ignored in most scenarios.

Synopsis:


```
    def preview_data(self,
                     output: Union[MarshalingView, MarshalerNode],
                     limit: int = None,
                     columns: int = None) -> (Any, DataFrameFormat):
```

Example:

```
data, format = app.preview_data(repeat_ducsi[3]['2021-01-12'], limit=10, columns=3)
```


***Application::load_data:*** Loads the data into the active Python context from remote resource with the most convenient and abstracted way. The size and the format of the returned data can also be controlled by parameters to this API.

If the intentions is to avoid loading the entire data, then ‘iterate’ parameter should be set as ‘True’ and for each partition (number of which is ultimately determined by ‘limit’ parameter) the returned iterator should be iterated multiple times. Otherwise, RheocerOS will auto-merge the result (even if the result spans multiple partitions) and return it. In that case, only one iteration is enough.

‘spark’ parameter should be passed in for the best result and reuse of existing session for ‘format=DataFrameformat.SPARK’ case.

Synopsis:

```
     def load_data(self,
                  output: Union[MarshalingView, MarshalerNode],
                  limit: int = None,
                  format: Union[DataFrameFormat, str] = None,
                  spark: 'SparkSession' = None,
                  iterate: bool = False) -> Iterator[Any]:
```

Example:

```
data_it = app.load_data(repeat_ducsi[3]['2021-01-12'], limit=10, format=DataFrameFormat.PANDAS)
data = next(data_it)
data = next(data_it)
```



## Application Termination

When you call ***Application::terminate***, the entire [Application Activation](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAkUrgT) process is reverted (analogous to a tear-down operation in CloudFormation), except the underlying ‘data’ storage. 

You need to call ***Application::delete* **in order to wipe-out the data (partitions of internal datasets, models) that has been created so far by your application.

A sequence of calls like;


```
app.terminate()
app.delete()
```


should not leave any trace in your AWS account. This functionality is very important for some of integ-test scenarios as well.

***Application::terminate***: Do a graceful shutdown of the whole application. Graceful in a way that it is logically the inverse of activation. It is comprehensive, guaranteed to reverse the activation (if any). This approach enables us to achieve reuse in low-level termination sequences as well.

It is like going back to a state where it can be assumed that there has been no activations before, keeping the data from the active state intact. It does not change the current development context but nullifies the active state. So in order to activate again, same application object can be reused to call 'activate' yielding the same active state from before the termination.

And more importantly, it is okay to make repetitive calls to this API. It is safe to do that even in scenarios where the workflow fails and another attempt is intended to complete the termination.

But it is not safe to call this API concurrently (on the same machine or from other endpoints).

Synopsis:

```
def terminate(self, wait_for_active_routes=False) -> None:
```

Setting ‘*wait_for_active_routes*’ as True will make termination orchestration to pause the application and then keep waiting for active executions to complete (either succeed or fail). 

***Application::delete***: Delete the remaining resources/data of an inactive (or terminated) app.

Why is this separate from 'terminate' API?

Because, we want to separate the deallocation of runtime resources from internal data which might in some scenarios intended to outlive an RheocerOS application. A safe-guard against deleting peta-bytes of data as part of the termination. Independent from the app, data might be used by other purposes. Or more importantly, same data can be hooked up by the reincarnated version of the same application (as long as the ID and platform configuration match). So we are providing this flexibility to the user.

And in a more trivial scenario where an application needs to be deleted before even an activation happens, then this API can be used to clean up the resources (e.g application data in storage). Because, RheocerOS applications are distributed right from begining (instantiation), even before the activation and might require resource allocation.

Upon successful execution of this command, using this Application object will not be allowed. If the same internal data is intended to be reused again, then a new Application object with the same ID and platform configuration should be created.


## Application Execution Control

***Application::execute***: Execute a particular node using its materialized version or with materialized inputs. Causes remote executions on the compute targets declared as part of the input ‘data_node’ and also activates/updates the application if it is not already or if any change is detected.

It is by default a blocking call, waits for the execution to succeed (or fail) and polls the system. During the poll, the user is informed continously (based on the current console or file logging configuration). If the result is successful, returns the physical path of the execution output.

Synopsis:


```
 def execute(self,
             data_node,
             [materialized_input_nodes]) -> Optional[str]:
```


second input is optional if the data_node is already materialized (with concrete dimension values).


Example:


```
physical_path = app.execute(my_etl_node['NA']['2020-12-01'])

print(physical_path)

# s3://if-my-app-111222333444-us-east-1/internal_data/my_etl_node/NA/2020-12-01
```


***Application::process***: Injects a raw event or a materialized version of a data_node (signal) into the system. Based on the input declaration of all of the internal nodes (routes), they will get triggered. 

Raw event can be anything that your current configuration supports. So for instance, in AWS configuration case (with the default drivers), you can use raw events from S3, SNS, EventBridge (also for Andes BDT events).

Any local upstream reference from a create_data call or any reference retrieved via [Finding Nodes](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAFH1ND) can also be used as an input to this API as long as concrete dimension values are given or the node/signal is materialized (including no dimensions case).

When a target node is to be triggered, its inputs are provided to Application::process to satisfy the system, the trigger condition actually.

It returns a map of impacted (triggered) routes and execution ids.

Synopsis:


```
 def process([data_node | raw_event])
```


Example:


```
new_data = app.create_data('my_new', inputs= [app['my_data']],
                           compute_targets="output = my_data.filter(col('day') < ...")

app.activate() # new data won't be used if not activated already

app.process(app['my_data']['2020-01-02'])

# this will trigger an execution on 'new_new' data over partition '2020-01-02'


```


***Application::poll***: This api is used to make sure that an output partition has been successfully created. If there is any ongoing executions on it, then it keeps polling and informing the user (based on the current console or file logging configuration).

This API is similar to Application::execute, but it never triggers a new execution.

If the partition does not exist or the most recent execution has failed, then it returns ‘None’.

Synopsis:


```
def poll(data_node) -> Optional[str]:
```


Example:


```
physical_path = app.poll(my_data['NA']['2020-01-01')

print(phyiscal_path)

# s3://if-my-app-111222333444-us-east-1/internal_data/my_data/NA/2020-01-01
```


***Application::materialize:***

This API is needed to keep the users agnostic from the internal resources (storage technologies, etc). It is used to map upstream or internal data node partitions to their physical, universal path formats.

It is similar to poll and execute APIs in terms of return value however it does not guarantee the existence of the target partition. It stands as a translation layer.

If the first parameter is not materialized (with concrete dim values) or not dimensionless, then the second parameter should be used. If not then path generation is aborted. If you still want to generate the logical path in such a case, then provide special pameters , ‘*’ for any and relative indexes ( like [-3] for the last three days ).

Synopsis:


```
def materialize(data_node, [materialized dimension/partition values]) -> Optional[str]:
```


Examples:


```
path = app.materialize(data_node['NA']['2020-01-01])

# load path into Spark or Pandas dataframe
```



## Connecting with other Applications

When you have multiple RheocerOS applications in your ecosystem, you will want to connect them to each other to share data.

Again the applications form a hierarchy similar to their internal DAGs. When you want to consume data from another application, you expect them to use “**Application::export_to_downsteam_application**” API. Then you use “**Application::import_upstream_application**” API to finalize the connection.

Once the connection is established, you can use the data from your upstream application seamlessly, as if they are as part of your own DAG (as if they were created in a previous ‘create_data’ API call in the same application).

### AWS: 

Note: When you want to be authorized by an upstream application, your application should be activated first. So it should basically exist (provisioned, its [AWS] resources [such as IAM roles, etc] should be created).


* call ‘activate’ on your application (you don’t need to have any nodes).
* make sure that your upstream application adds a line in their app code with ‘**authorize_downstream**’ with parameters for your app_id, account_id and region_id.
    * Then upstream application should be ‘activate’d.
* Now in your application you can add ‘**import_upstream**’ line with parameters for upstream app_id, account_id and region_id.
    * You should add this line before the API calls where you’d like to use data nodes from the upsteam app.
* You can retrieve data nodes either from your own (using retrieval APIs) or directly using the return value (remote application object reference) from ‘**import_upstream**’ call.


**AWSApplication::authorize_downstream**:

Authorize another RheocerOS application.

Synopsis:


```
def authorize_downstream(remote_app_name: str,
                       aws_acc_id: str,
                       region_id: str) -> None:
```


Example:


```
  app = AWSApplication('joe_does_FEATURE_REPO', 'us-east-1', '999999999999')
  app.authorize_downstream('jane_does_IF', '111222333444', 'us-east-1')
```


**AWSApplication::import_upstream**:

Import another RheocerOS application which has already authorized you with ‘**AWSApplication::authorize_downstream**’ API.

Synopsis:


```
def import_upstream(remote_app_name: str,
                       aws_acc_id: str,
                       region_id: str) -> RemoteApplication:
```

return value ‘RemoteApplication’ provides contrained sub-set of APIs from Application class. You can use all of the retrieval APIs and the analysis on its routes, etc.

Example:


```
# please note how the parametrization here mathces the example from 
# authorize_downstream example above
downstream_app = AWSApplication('jane_does_IF', 'us-east-1', '111222333444')

repo_app = downstream_app.import_upstream('joe_does_FEATURE_REPO', 'us-east-1', '999999999999')

# assume foo_feature exists only in the metadata of a dataset node from repo
data_nodes = repo_app.search_data('foo_feature')

data_nodes2 = app.search_data('foo_feature')

assert len(data_nodes) == len(data_nodes2)

foo_from_repo = repo_app['foo_table']
foo = app['foo_table']

assert foo_from_repo == foo

# now use that upstream data node as an input in this downstream app

my_etl = app.create_data('etl_dat_over_foo',
                         inputs=[foo],
                         compute_targets='''spark.sql(\"\"\"
                         
                                select * from foo_table where foo_feature = 5
                                
                                \"\"\")''')

output_partition_path = app.execute(my_etl[datetime.now()]
     

```



## Application Security

RheocerOS adapts and implements [“InfoSec Data Protection Model”](https://skb.highcastle.a2z.com/guides/88) for its architecture. The main components of a generic RheocerOS application can be configured for security based on the mental model suggested by InfoSec. So from the user’s perspective (without knowing about the underlying drivers/impls), the following framework provided Python types can be used to refer to the architectural components of an application:


* <Storage>
* <RoutingTable>
* <BatchCompute>
* <ProcessingUnit>
* <ProcessorQueue>

And for the configuration of security per component (per PASSING, PROCESSING or PERSISTING), the following entity model is used:

```
class DataHandlingType(str, Enum):
    """https://skb.highcastle.a2z.com/guides/88#known-systems"""
    PASSING = "PASSING"
    PROCESSING = "PROCESSING"
    PERSISTING = "PERSISTING"


class EncryptionKeyAllocationLevel(str, Enum):
    HIGH = "ALL"
    PER_RESOURCE = "PER_RESOURCE"


class ConstructEncryption(NamedTuple):
    key_allocation_level: EncryptionKeyAllocationLevel
    """None values mean 'use default' in Construct impl"""
    key_rotation_cycle_in_days: int
    """With hard rotation, old data encrypted with prev key cannot be decrpyted with the new one.
    So a re-encryption might required with the new key. For example, KMS default rotation is NOT hard."""
    is_hard_rotation: bool
    """Controls the behaviour on old data when 'is_hard_rotation' is enabled."""
    reencrypt_old_data_during_hard_rotation: bool
    """e.g do you want root access to your KMS key in AWS case?"""
    trust_access_from_same_root: bool


class ConstructPersistenceSecurityDef(NamedTuple):
    encryption: ConstructEncryption


class ConstructPassingSecurityDef(NamedTuple):
    protocol: str


class ConstructProcessingSecurityDef(NamedTuple):
    zero_sensitive_data_after_use: bool
    enforce_privilege_separation: bool


class ConstructSecurityConf(NamedTuple):
    persisting: Optional[ConstructPersistenceSecurityDef]
    passing: Optional[ConstructPassingSecurityDef]
    processing: Optional[ConstructProcessingSecurityDef]
```


Altogether, to add a security conf for a component, the following Application API is used:

***Application::set_security_conf***: 

Synopsis:


```
def set_security_conf(self, 
                      construct_type: Type[BaseConstruct],
                      conf: ConstructSecurityConf) -> None:
```


**Params**:

_construct_type_: Can be one of [Storage, RoutingTable, ProcessingUnit, ProcessorQueue, BatchCompute]
_conf_: Define three main aspects of a the system component using the entity model presented above.

**Example**: In this example, we have an existing upstream (parent) data-pipeline which needs to be modified in terms of security only. We attach to the application, load existing DAGs, configurations (if any) and overwrite its security configuration for its _Storage_ component which is used to storage intermediate datasets and application’s own data (DAG conf, node metadata, drivers, etc).

If the Storage driver is based on S3, then the this change means enabling of cross-account KMS key with the finer granular control on it. Please check the parameters for ConstructPersistenceSecurity entity below.

Once the the application is activated, downstream RheocerOS applications (e.g data-pipelines) will have no distruption and require no configuration to keep consuming events and accessing data from the S3 bucket of this application. Connecting tissue RheocerOS will manage everything, bucket, KMS policies and on the downstream side IAM role policy modifications, etc.


```
    parent_app = AWSApplication("eureka-dev-yunusko", "us-east-1", dex_dev_account)
    parent_app.attach()
    parent_app.set_security_conf(Storage, ConstructSecurityConf(
                                           persisting=ConstructPersistenceSecurityDef(
                                              ConstructEncryption(EncryptionKeyAllocationLevel.HIGH,
                                                                   key_rotation_cycle_in_days=60,
                                                                   is_hard_rotation=False,
                                                                   reencrypt_old_data_during_hard_rotation=False,
                                                                   trust_access_from_same_root=False)),
                                              passing=None,
                                              processing=None))
    # deploy, activate                                            
    parent_app.activate()
```


Please check this [secure collaboration example from RheocerOS package](https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/examples/secure_collaboration_between_eureka_andes_node_examples.py) to get an idea about the bigger picture.

## Application Alarming and Metrics (in Data Pipelines / ML Workflows)

Doc: [RheocerOS Alarms & Metrics Extensions](https://quip-amazon.com/OhTnA77Iaf7i)

Full tutorial on alarming and metrics for a sample project: https://broadcast.amazon.com/videos/430680

* Deep-dive in related entity model in RheocerOS, help new team-members build intuition around ‘OE as code, as part of flow’ paradigm
* Custom metrics, system, orchestration and data-pipeline node specific metrics
* How to emit custom metrics in node hooks (callbacks) or in Spark code


RheocerOS extends the same programming model used against entities (such as datasets, timers) for metrics and alarms and turns them into signals so that:

* external metrics (i.e Cloudwatch, PMET) or internal (user defined custom metrics) could be bound to alarms. An alarm becomes a new signal. It is possible to define default actions (SIM, Email) to an alarm if it is not going to be inputted to any downstream compute node (i.e data node) as a trigger.
    * internal (user defined custom) metrics emitted in batch compute or inlined/lambda executions (with the same convenience of Coral metrics).
    * internal custom metrics can also be emitted from user provided hooks to node callbacks (execution begin, success, failure, retry, etc).
* metrics for RheocerOS generated resources (AWS Lambda, Glue, DynamoDB, etc) can be bound to user defined alarms while still keeping the infrastructure abstraction from user’s perspective.
* orchestration and node based metrics automatically emitted by RheocerOS from your platform (AWS).
    * Node Examples: ‘my_data_node.exec_failed’  (count metric), ‘my_data_node.exec_succeeded’, similar default metrics for latency, etc.
    * High-level: ‘event.type’ (AWS event type ingested by RheocerOS).
* alarms could be bound to composite alarms. A composite alarm becomes a new signal. It is possible to define default actions (SIM, Email) to a composite alarm if it is not going to be inputted to any downstream compute node (i.e data node) as a trigger.
* then alarms and composite alarms could be bound to other nodes via Application::create_data as an input to build more complex monitoring or reactor logic (batch compute or an inlined/lam compute based on a state-change of an alarm along with other inputs for example).


RheocerOS pulls alarming&metrics into application development (also testing!) and runtime as first-class citizen software constructs. They are now well integrated with the rest of the entity model, flow and managed by same orchestration. This eliminates disconnected OE conf (pretty much what the rest of Amazon is doing today). This is a major innovation and the elimination of a culprit to low development scalability, lack of tests of alarming/metrics, high-risk system maintenance and more importantly the underlying cause of many COEs.

Also RheocerOS creates a default dashboard for your application’s OE configuration (for AWS, as an application specific CW dashboard), along with default system, orchestration metrics.

### Alarming and Metrics in Project QASP (C2P):

A productionized example of utilizing alarming and metrics extensions of RheocerOS for C2P data-pipeline. Alarming and metric is as part of the application flow.

* Availability: https://code.amazon.com/reviews/CR-61409360/revisions/8#/details
    * Orchestration provided metrics
    * New custom metrics
* Latency: https://code.amazon.com/reviews/CR-61826466/revisions/2#/details


Design: [QASP KPIs, Metrics, and Alarms](https://quip-amazon.com/NEV7AZwKCXOr)

Please contact [](https://phonetool.amazon.com/users/gaudynd)[Dominique Gaudyn](https://quip-amazon.com/BeN9EAzVQnX) for more details and resources.

### Other Examples:

Here is an RheocerOS application focusing only on Alarming and Metrics capabilities. Its main purpose is to track system failures, a specific Lambda’s health with different alarms in terms of thresholds, input metrics, etc. And finally it shows how to combine alarms into a single composite alarm and then bind the composite alarm to a compute action. Please note that dedupe string is supported so redundant default actions on alarms and the default action on composite alarm don’t cause extra ticketing in this scenario.

Please also note that this oversimplified app code can be extended with actual data pipeline code (create_data calls or others).

Run the following code in a notebook or run it on your dev-box via:

*>> brazil-runtime-exec python application/alarming_and_metrics_example.py*


```
from datetime import datetime

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueWorkerType

flow.init_basic_logging()
flow.init_config()

app_name = "alarming-ex"
app = AWSApplication(app_name, "us-east-1", "842027028048")

cwa_cti = CWAInternal.CTI(category="Deliery Experience",
                          type="Machine Learning",
                          item="Platform")

# METRICS
# import a metric definition from the same account
# generic representation of the metrics from this particular lambda.
external_lambda_metric = app.marshal_external_metric(external_metric_desc=CWMetric(namespace="AWS/Lambda"),
                                              id="lambda_metric",
                                              sub_dimensions={
                                                  "FunctionName": "my-test-function"
                                              })

# import the same metric in a different, more flexible way.
#   This shows Lambda Error metric can be imported into the system in
#   different ways (different IDs, default alias').
#   And also with defaults/overwrites on metric dimensions
external_lambda_error_metric_on_another_func = app.marshal_external_metric(
                                               external_metric_desc=CWMetric(namespace="AWS/Lambda"),
                                               id="my_test_function_error",
                                               dimension_filter=\
                                               {
                                                   "Error": {  # only keep 'Error'
                                                       MetricStatistic.SUM: {  # support SUM only
                                                           MetricPeriod.MINUTES(5): {  # restrict the use of this metric with 5 mins period only (in alarms)
                                                               "*": {  # (reserved) Any MetricDimension.TIME
                                                               }
                                                           }
                                                       }
                                                   }
                                               },
                                               sub_dimensions={
                                                   "FunctionName": "my-test-function-2"
                                               })

internal_spark_error_metric = app.create_metric(id="my_custom_spark_error_metric",
                                    dimension_filter={
                                        "Error": {  # only keep 'Error'
                                            "*": {  # support all statistic (no filter spec)
                                                "*": {  # support all periods (during the emission sets Storage Resolution to 1)
                                                    "*": {  # (reserved) Any MetricDimension.TIME
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    )

# generic metric declaration that can be used to emit metrics and bind them into alarms in a less restrictive way.
generic_internal_metric = app.create_metric(id="my_app_error")


# ALARMS
error_system_level_dedupe_string = f"{app_name}-ERROR"

# ALARM with default ticket action
etl_error_alarm = app.create_alarm(id="one_or_more_spark_executions_failed",
                                   target_metric_or_expression=internal_spark_error_metric['Error'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1,
                                   default_actions=AlarmDefaultActionsMap(
                                                   ALARM_ACTIONS={
                                                       CWAInternal(cti=cwa_cti, dedupe_str=error_system_level_dedupe_string)
                                                   },
                                                   OK_ACTIONS=set(),
                                                   INSUFFICIENT_DATA_ACTIONS=set()
                                                   )
                                   )

generic_internal_alarm = app.create_alarm(id="generic_error_alarm",
                                   #  refer
                                   #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
                                   target_metric_or_expression="errors > 0 OR failures > 0",  # returns a time series with each point either 1 or 0
                                   metrics={
                                       "errors": generic_internal_metric['Error'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                       "failures": generic_internal_metric['Failure'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
                                   },
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1,
                                   default_actions=AlarmDefaultActionsMap(
                                       ALARM_ACTIONS={
                                           CWAInternal(
                                               cti=cwa_cti,
                                               severity=CWAInternal.Severity.SEV3,
                                               action_type=CWAInternal.ActionType.TICKET,
                                               dedupe_str=error_system_level_dedupe_string
                                           )
                                       })
                                   )

# no default actions on this one, will be used in a composite alarm.
system_failure_alarm = app.create_alarm(
                                 id="system_failure",
                                 #  refer
                                 #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
                                 target_metric_or_expression="SUM(METRICS())",
                                 # will validate if metrics are materialized (i.e NAME, Statistic and Period dimensions are material or not).
                                 metrics=[external_lambda_metric['Error'][MetricStatistic.SUM], external_lambda_error_metric_on_another_func],
                                 number_of_evaluation_periods=5,
                                 number_of_datapoint_periods=3,
                                 comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                 threshold=1
                                 )

composite_alarm = app.create_composite_alarm(id="system_monitor",
                                             # equivalent logic
                                             # alarm_rule=~(etl_error_alarm['OK'] & generic_internal_alarm['OK'] & system_failure_alarm['OK']),
                                             alarm_rule=etl_error_alarm | generic_internal_alarm['ALARM'] | ~system_failure_alarm[AlarmState.OK],
                                             default_actions = AlarmDefaultActionsMap(
                                                ALARM_ACTIONS={
                                                    CWAInternal(
                                                        cti=cwa_cti,
                                                        severity=CWAInternal.Severity.SEV3,
                                                        action_type=CWAInternal.ActionType.TICKET,
                                                        dedupe_str=error_system_level_dedupe_string
                                                    )
                                                })
                                            )


class MonitorFailureReactorLambda(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: Dict[str, Any]) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

        print(f"Hello from AWS Lambda account_id {params[AWSCommonParams.ACCOUNT_ID]}, region {params[AWSCommonParams.REGION]}")
        s3 = params[AWSCommonParams.BOTO_SESSION].client("s3")
        cw = params[AWSCommonParams.BOTO_SESSION].client("cloudwatch")

        # do extra stuff when the following monitor ('system_failure_reactor') is in alarm.
        # TODO
        return

class MonitorReactorLambda(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: Dict[str, Any]) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
        from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmDimension

        composite_alarm_signal = input_map.values()[0]
        print(f"Reacting to {composite_alarm_signal.get_materialized_resource_paths()[0]} in account_id {params[AWSCommonParams.ACCOUNT_ID]}, region {params[AWSCommonParams.REGION]}")

        "'OK' or 'ALARM' or 'INSUFFICIENT_DATA'"
        current_alarm_state: str = composite_alarm_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        print(f"New state of alarm {composite_alarm_signal.alias} is {current_alarm_state!r}.")

        s3 = params[AWSCommonParams.BOTO_SESSION].client("s3")
        cw = params[AWSCommonParams.BOTO_SESSION].client("cloudwatch")

        # do extra stuff when the monitor ('system_failure_reactor') is either in alarm or ok state.
        # TODO
        return

monitor_failure_reactor = app.create_data(id="system_failure_reactor",
                                  inputs=[composite_alarm[AlarmState.ALARM]], # when transitions into ALARM state
                                  compute_targets=[
                                      InlinedCompute(MonitorFailureReactorLambda())
                                  ])

monitor_reactor = app.create_data(id="system_monitor_reactor",
                                  inputs=[composite_alarm['*']],  # will get triggered in both cases
                                  compute_targets=[
                                      InlinedCompute(MonitorReactorLambda())
                                  ])

app.activate(allow_concurrent_executions=False)
```

## End-2-End Features

### Backfilling

C2P/QASP data pipeline backfiller tool: https://code.amazon.com/reviews/CR-62039240

Can be easily modified to serve any other application/topology. A good example to create other e2e mechanisms for your pipeline.

### Visualization

Track [2.2 Visualization (Self-serve): RheocerOS RoadMap 2022 (AI Foundations)](https://quip-amazon.com/7QXcAHXpbPct#temp:C:EUWcef8967d69f39211fbf47a022)

## Use Cases

### Migrate Eider Notebook (with external S3 and Andes data) to AWS Sagemaker Notebook

Broadcast video: https://broadcast.amazon.com/videos/323047

This effort was done in **less than an hour** based on a request from one of our senior scientists ([Anupama Kochrekar](https://quip-amazon.com/JVW9EAJWbv8)) because she was facing issues in Eider (data volatility, long execution times, no support from Eider, etc). 

All of the datasets needed were generated and persisted in AWS in less than **10 mins**, unlocking the power of AWS Glue v2.0 and scalability, and RheocerOS’s event propagation (push model from a timer signal down to the last node).

Scientist’s Notebook:

https://eider.corp.amazon.com/kochreka/notebook/output/NBZGTIEDZANI

Steps taken:


* (CONDITIONAL if this is your first time with RheocerOS) Provision notebook instance that will contain RheocerOS kernel: Refer [Quick Experimentation with CDO Data (Andes) on Dev-Box and then Continuing in Sagemaker Notebook Instance](https://quip-amazon.com/HkWMAcP3b6My#GKW9CAm44AV)
    * Create a notebook using ‘RheocerOS’ kernel (at the root level for example).
    * We ended up with:
        * https://adpd-training-notebook.notebook.us-east-1.sagemaker.aws/notebooks/adpd-training-notebook.ipynb
* (CONDITIONAL if S3 bucket is owned by another AWS account) Make sure that external S3 bucket gives necessary permissions to our account root **or** the dev and exec roles that will be created for your notebook:
    * AWS Role: {your_app_name}-{your app region}-RheocerOSDevRole
    * AWS Role: {your_app_name}-{your app region}-RheocerOSExeRole
    * For this migration example that would look like:
        * arn:aws:iam::427809481713:role/adpd-training-us-east-1-RheocerOSDevRole
        * arn:aws:iam::427809481713:role/adpd-training-us-east-1-RheocerOSExeRole
* (CONDITIONAL if needed Andes table is not sync’ed into your account) Sync [D_PERFECTMILE_PACKAGE_ITEMS_NA](https://hoot.corp.amazon.com/providers/2b9355d5-33dc-4d7a-8c2a-c486549a6235/tables/D_PERFECTMILE_PACKAGE_ITEMS_NA/versions/4) following [these steps](https://quip-amazon.com/HkWMAcP3b6My/RheocerOS-User-Guide#GKW9CAWlLIX).
* Copy and paste the following code into the [notebook](https://adpd-training-notebook.notebook.us-east-1.sagemaker.aws/notebooks/adpd-training-notebook.ipynb) (into one cell or multiple cell for each API [based on your preference])
    * Execute the cell or the cells in sequential order, then in 7-8 mins all of the new partitions for datasets for week ending 2021-01-16 will be ready in https://s3.console.aws.amazon.com/s3/buckets/if-adpd-training-427809481713-us-east-1?prefix=internal_data/
    * The reason for data generation is the ‘impatient’ / ‘retrospective’ Application::process call that synthetically feeds the system with the timer  signal and causes an execution chain starting with ingestion from S3 and Andes and all the way down to ‘ramp’ dataset/node. Otherwise, you wil have to wait for daily_timer to kick in each week (see Application::add_timer schedule_expression below).
* from datetime import datetime
    
    import intelliflow.api_ext as flow
    from intelliflow.api_ext import *
    from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueWorkerType
    
    flow.init_basic_logging()
    flow.init_config()
    
    PIPELINE_DAY_DIMENSION="day"
    ADPD_RANGE_IN_DAYS = 7
    REGION_ID = 1
    MARKETPLACE_ID = 1
    PERFECTMILE_TABLE_MAP = {1: "d_perfectmile_package_items_na", 2: "d_perfectmile_package_items_eu"}
    
    app = AWSApplication("adpd-training", "us-east-1", "427809481713")
    
    # daily timer
    daily_timer = app.add_timer("daily_timer",
                                "rate(7 days)",
                                time_dimension_id=PIPELINE_DAY_DIMENSION)
    
    denorm_archive_ad = app.create_data("denorm_archive_ad",
                                     inputs=[daily_timer],
                                     compute_targets=[
                                         BatchCompute(
                                             code=f"""
    from datetime import datetime, timedelta
    import dateutil.parser
    from pyspark.sql.functions import col
    
    day = dimensions["{PIPELINE_DAY_DIMENSION}"]
    date = dateutil.parser.parse(day)
    
    # get the last 7 days (starting from partition day and going backwards)
    date_list = list(date - timedelta(days=i) for i in range({ADPD_RANGE_IN_DAYS}))
    
    def create_path(date_list):
        date_list = [d.strftime('%Y-%m-%d') for d in date_list]
        denorm_archive_s3_path_start = 's3://fulfillmentdataanalyticslambd-splitparquetresults-zzl62x5qp21a/denormalized_archive/creation_date='
        denorm_archive_s3_path_end = '/region_id={REGION_ID}/system=F2P/'
        s3_path = list()
        hours = ['-00', '-01', '-02', '-03', '-04', '-05', '-06', '-07', '-08', '-09', '-10', '-11', '-12', '-13', '-14',
                 '-15', '-16', '-17', '-18', '-19', '-20', '-21', '-22', '-23']
        quarters = ['-00', '-15', '-30', '-45']
        for i in date_list:
            for j in hours:
                for k in quarters:
                    s3_path.append(denorm_archive_s3_path_start + i + j + k + denorm_archive_s3_path_end)
    
        return s3_path
    
    s3_paths = create_path(date_list)
    
    # IF_MIGRATION_FIX sync with Anupama 'ready_date' and 'supply_type' were duplicates here
    spark.read.parquet(*s3_paths).select("fc_name", "iaid", "planning_date", "ramp_cost", "order_id", "order_date", "ship_method",
                                                  "processing_hold_date" , "expected_ship_date", "expected_arrival_date", "internal_pdd",
                                                  "internal_psd", "fulfillment_request_id",  "marketplace_id", "context_name", "order_fta_flag",
                                                  "fulfillment_fta_flag", "planned_shipment_id", "shipment_id", "redecide_flag",
                                                  "fulfillment_reference_id", "ship_cpu", "zip_code", "supply_type", "transship_from", 
                                                  "transship_cpt",  "shipgen_deadline", "gl_product_group", "release_date", "split_promise",
                                                  "hazmat_flag", "atrops_penalty_cost", "delivery_promise_authoritative","box_type","boxes_per_plan",
                                                  "our_price", "fc_type", "boxes_per_order", "promises_per_order","supply_time", "line_item_id",
                                                  "address_id", "units_per_plan", "gift_wrap_flag", "surety_duration",  "delivery_group",
                                                  "units_per_shipment", "promise_quality", "supply_ft_flag", "iog", "orders_per_shipment",
                                                  "box_dimensions", "box_volume", "ship_weight", "pallet_ship_method_flag", "processing_hold_type",
                                                  "plan_transship_cost", "plan_opportunity_cost", "previous_fc", "ready_date", "redecide_reason", 
                                                  "ship_promise_authoritative", "units_per_order", "delivery_oriented_flag", "ship_option" ) \\
                                 .createOrReplaceTempView('denorm_archive')
    
    output = spark.sql(
        "select * from denorm_archive where ship_option = 'second-nominated-day' and marketplace_id={MARKETPLACE_ID} and context_name = 'f2p_production' ")
                                             """,
                                             extra_permissions=[Permission(resource=['arn:aws:s3:::fulfillmentdataanalyticslambd-splitparquetresults-zzl62x5qp21a/denormalized_archive/*'],
                                                                           action=['s3:Get*'])
                                                                ],
                                             lang=Lang.PYTHON,
                                             GlueVersion="2.0",
                                             WorkerType=GlueWorkerType.G_1X.value,
                                             NumberOfWorkers=200,
                                             Timeout=10 * 60  # 10 hours
                                         )
                                     ])
    
    
    d_perfectmile_package_items_na = app.marshal_external_data(
                                        AndesDataset("perfectmile",
                                                     PERFECTMILE_TABLE_MAP.get(REGION_ID),
                                                     partition_keys=["ship_day"],
                                                     primary_keys=["fulfillment_shipment_id", "customer_shipment_item_id"],
                                                     table_type="APPEND")
                                        , "d_perfectmile_package_items_na"
                                        , {
                                            PIPELINE_DAY_DIMENSION: {
                                                type: DimensionType.DATETIME,
                                                "format": "%Y-%m-%d"
                                            }
                                        }
                                        , {
                                            '*': {
                                            }
                                        }
                                    )
    
    dppi_df = app.create_data("dppi_df",
                                        inputs={"timer": daily_timer,
                                                # read the last 7 days from the day of timer
                                                "dppi_low_range": d_perfectmile_package_items_na[:-ADPD_RANGE_IN_DAYS].ref,
                                                # try to read 14 days into the future for shipment data
                                                "dppi_high_range": d_perfectmile_package_items_na[:14].ref},
                                        compute_targets=[
                                            BatchCompute(
                                                code=f"""
    import dateutil.parser
    from datetime import datetime, timedelta
    
    start_day = dimensions["{PIPELINE_DAY_DIMENSION}"]
    start_date = dateutil.parser.parse(start_day)
    
    end_date = start_date - timedelta({ADPD_RANGE_IN_DAYS})
    end_day = end_date.strftime('%Y-%m-%d')
    
    dppi_low_range.unionAll(dppi_high_range).createOrReplaceTempView('dppi_full_range')
    
    output = spark.sql(f'''
    SELECT ordering_order_id,
        customer_order_item_id,
        ship_day, 
        rcnt_supply_category_type_id, 
        asin, 
        order_datetime, 
        fulfillment_reference_id, 
        is_instock, 
        carrier_sort_code, 
        clock_stop_event_datetime_utc, 
        warehouse_id, 
        package_id, 
        internal_promised_delivery_date_item, 
        ship_method, 
        customer_ship_option, 
        quantity, 
        ship_date_utc, 
        is_hb, 
        promise_data_source, 
        planned_shipment_id, 
        fulfillment_shipment_id, 
        estimated_arrival_date, 
        event_timezone, 
        marketplace_id, 
        ship_option_group, 
        customer_shipment_item_id
        FROM dppi_full_range
            WHERE 1=1
                and region_id = {REGION_ID}
                AND marketplace_id = {MARKETPLACE_ID} 
                and customer_ship_option = 'second-nominated-day'
                and cast(order_datetime as date) >= "{{end_day}}"
                and cast(order_datetime as date) <= "{{start_day}}"
                       ''')
    
                                             """,
                                                lang=Lang.PYTHON,
                                                GlueVersion="1.0",  # EDX BASED ANDES DATA ACCESS on Glue 1.0
                                                WorkerType=GlueWorkerType.G_1X.value,
                                                NumberOfWorkers=100,
                                                Timeout=8 * 60
                                            )
                                        ])
    
    slam_broadcast = app.create_data("slam_broadcast",
                              inputs=[dppi_df],
                              compute_targets=[
                                  BatchCompute(
                                      code=f"""
    
    output = spark.sql("select ordering_order_id, asin, max(planned_shipment_id) as planned_shipment_id,max(FULFILLMENT_REFERENCE_ID) as FULFILLMENT_REFERENCE_ID from dppi_df  group by ordering_order_id, asin")
    
                                             """,
                                      lang=Lang.PYTHON,
                                      GlueVersion="2.0",
                                      WorkerType=GlueWorkerType.G_1X.value,
                                      NumberOfWorkers=150,
                                      Timeout=8 * 60
                                  )
                              ])
    
    plans_df = app.create_data("plans_df",
                                 inputs=[denorm_archive_ad,
                                         slam_broadcast],
                                 compute_targets=[
                                     BatchCompute(
                                         code=f"""
    
    output = spark.sql("select order_id, iaid, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN a.marketplace_id END) as fta_MARKETPLACE_ID, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN fc_name END) as FTA_FC_NAME, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN A.zip_code END) as FTA_zip_code,  MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN a.ship_method END) as FTA_ship_method, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN planning_date END) as FTA_planning_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN supply_type END) as FTA_supply_type, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN transship_from END) as FTA_transship_from,  MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN ship_cpu END) as FTA_ship_cpu, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN transship_cpt END) as FTA_transship_cpt, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN order_date END) as FTA_order_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN internal_psd END) as FTA_internal_psd, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN internal_pdd END) as FTA_internal_pdd, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN expected_ship_date END) as FTA_expected_ship_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN expected_arrival_date END) as FTA_expected_arrival_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN ready_date END) as FTA_ready_date, MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN shipgen_deadline END) as FTA_shipgen_deadline, CASE WHEN MIN( CASE WHEN order_fta_flag = 1 AND fulfillment_fta_flag = 1 AND redecide_flag = 0 THEN TRANSSHIP_FROM END) IS NOT NULL THEN 'Y' ELSE 'N' END AS IS_FTA_TRANSSHIP, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.marketplace_id END) as LTA_MARKETPLACE_ID, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fc_name END) as LTA_fc_name, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.zip_code END) as LTA_zip_code, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_method END) as LTA_ship_method, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.planning_date END) as LTA_planning_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.supply_type END) as LTA_supply_type,  max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.transship_from END) as LTA_transship_from, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_cpu END) as LTA_ship_cpu,  max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.box_volume END) as LTA_box_volume, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.processing_hold_type END) as LTA_processing_hold_type, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ship_weight END) as LTA_ship_weight, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.order_date END) as LTA_order_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.promise_quality END) as LTA_promise_quality, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.internal_psd END) as LTA_internal_psd, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.internal_pdd END) as LTA_internal_pdd, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.expected_ship_date END) as LTA_expected_ship_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.expected_arrival_date END) as LTA_expected_arrival_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.gl_product_group END) as LTA_gl_product_group, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.ready_date END) as LTA_ready_date, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fulfillment_request_id END) as lta_fulfillment_request_id, MIN( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.shipgen_deadline END) as LTA_shipgen_deadline, CASE WHEN MAX( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN TRANSSHIP_FROM END) IS NOT NULL THEN 'Y' ELSE 'N' END as IS_LTA_TRANSSHIP, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.fc_type END) as LTA_fc_type, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.our_price END) as lta_our_price, max( CASE WHEN a.PLANNED_SHIPMENT_ID = b.PLANNED_SHIPMENT_ID AND a.FULFILLMENT_REFERENCE_ID = b.FULFILLMENT_REFERENCE_ID AND SHIPMENT_ID IS NULL THEN A.delivery_group END) as lta_delivery_group, MAX( CASE WHEN redecide_reason in ( 'MissedAllocationDeadline', 'FasttrackMissedAllocationDeadline', 'MissShipGenDeadline' , 'MissedShipGenDeadliFasttrackMissedShipgenDeadline' ) then 'Y' else 'N' END) as PLAN_EVER_MISSED_DEADLINE from denorm_archive_ad a join slam_broadcast b on b.ordering_order_id=a.order_id and iaid = b.asin group by order_id, iaid")
    
    
                                         """,
                                         lang=Lang.PYTHON,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=150,
                                         Timeout=15 * 60
                                     )
                                 ])
    
    
    slam_data_transformed = app.create_data("slam_data_transformed",
                               inputs=[dppi_df],
                               compute_targets=[
                                   BatchCompute(
                                       code=f'''
    
    output = spark.sql("""
        SELECT
            marketplace_id
            , ordering_order_id
            , asin
            , ship_day
            , fulfillment_shipment_id
            , fulfillment_reference_id
            , ship_date_utc
            ,from_utc_timestamp(ship_date_utc, 'PST') as ship_date_local
            , planned_shipment_id
            , warehouse_id
            , order_datetime
            , ship_method
            , quantity
            , package_id
            , customer_shipment_item_id
            , customer_ship_option
            , carrier_sort_code
            ,internal_promised_delivery_date_item
            , estimated_arrival_date
            , clock_stop_event_datetime_utc
            ,CASE WHEN clock_stop_event_datetime_utc is null THEN 'NoScan' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  > cast(internal_promised_delivery_date_item as date) THEN 'IntLate' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  < cast(internal_promised_delivery_date_item as date) THEN 'IntEarly' WHEN clock_stop_event_datetime_utc is not null and cast(from_utc_timestamp(clock_stop_event_datetime_utc, 'PST') as date)  = cast(internal_promised_delivery_date_item as date) THEN 'IntOnTime' END AS int_dea_status
        from dppi_df dosi""")
                                         ''',
                                       lang=Lang.PYTHON,
                                       GlueVersion="2.0",
                                       WorkerType=GlueWorkerType.G_1X.value,
                                       NumberOfWorkers=100,
                                       Timeout=10 * 60
                                   )
                               ])
    
    ramp = app.create_data("ramp",
                            inputs=[slam_data_transformed, plans_df],
                            compute_targets=[
                                BatchCompute(
                                    code=f'''
    
    output = spark.sql("""
        SELECT 
            order_day,
            ship_day,
            ship_date_utc,
            ship_date_local
            , lta_fc_type
            ,dosi.order_id
            ,dosi.asin
            ,ship_method as dosi_ship_method
            ,LTA_zip_code
            ,LTA_planning_date
            ,FTA_supply_type
            ,LTA_supply_type
            ,lta_ship_method
            ,lta_gl_product_group
            ,lta_ship_cpu
            ,fulfillment_shipment_id
            ,lta_fulfillment_request_id
            ,lta_promise_quality
            ,lta_ship_weight
            ,lta_box_volume
            ,lta_processing_hold_type
            ,lta_delivery_group
            ,lta_our_price
            , case 
                    when ship_method = 'AMZL_US_PREMIUM_AIR' then 'AMZL_AIR'
                    when ship_method = 'AMZL_US_PREMIUM' then 'AMZL_GROUND'
                    when substring(ship_method,0,4) = 'AMZL' then 'AMZL_Other'
                    when substring(ship_method,0,4) = 'UPS_' then 'UPS'
                    when substring(ship_method,0,5) = 'FEDEX' then 'FEDEX'
                    when substring(ship_method,0,5) = 'USPS_' then 'USPS'
                    else 'Others' end as dosi_carrier
            ,case 
                    when lta_ship_method = 'AMZL_US_PREMIUM_AIR' then 'AMZL_AIR'
                    when lta_ship_method = 'AMZL_US_PREMIUM' then 'AMZL_GROUND'
                    when substring(lta_ship_method,0,4) = 'AMZL' then 'AMZL_Other'
                    when substring(lta_ship_method,0,4) = 'UPS_' then 'UPS'
                    when substring(lta_ship_method,0,5) = 'FEDEX' then 'FEDEX'
                    when substring(lta_ship_method,0,5) = 'USPS_' then 'USPS'
                    else 'Others' end as lta_carrier
            ,case when ship_method IN ('AMZL_US_LMA_AIR','AMZL_US_PREMIUM_AIR','UPS_2ND','UPS_2ND_COM','UPS_2ND_COM_SIG','UPS_2ND_DAY','UPS_2ND_SAT','UPS_2ND_SIG','UPS_AIR_DIRECT','UPS_NEXT',
           'UPS_NEXT_COM','UPS_NEXT_COM_SIG','UPS_NEXT_DAY','UPS_NEXT_DAY_SAVER','UPS_NEXT_SAT','UPS_NEXT_SAT_SIG','UPS_NEXT_SAVER','UPS_NEXT_SAVER_COM','UPS_NEXT_SAVER_COM_SIG','UPS_NEXT_SIG',
            'UPS_NXT_SVR_SIG','UPS_SAT_2ND','UPS_SAT_2ND_COM','UPS_SAT_2ND_SIG','USPS_ATS_BPM_AIR','USPS_ATS_PARCEL_AIR','USPS_ATS_STD_AIR')   then 'Air' else 'Ground' end as Mode
            ,warehouse_id as dosi_warehouse_id
            , lta_fc_name
            , int_dea_status 
            ,from_utc_timestamp(lta_internal_psd, 'PST') as lta_internal_psd_local,
            from_utc_timestamp(lta_internal_pdd, 'PST') as lta_internal_pdd_local,
            from_utc_timestamp(LTA_expected_ship_date, 'PST') as LTA_expected_ship_date_local,
               from_utc_timestamp(LTA_expected_arrival_date, 'PST') as LTA_expected_arrival_date_local,
            internal_promised_delivery_date_item,
            estimated_arrival_date,
            clock_stop_event_datetime_utc,
            is_fta_transship,
            is_lta_transship,
            PLAN_EVER_MISSED_DEADLINE,
            quantity
            from
             (  select warehouse_id,
                    cast(order_datetime as date) as order_day,
                    ship_day,
                    ship_date_utc,
                    ship_date_local,
                    ship_method,
                    ordering_order_id as order_id
                    , asin
                    , int_dea_status 
                    ,internal_promised_delivery_date_item
                    ,estimated_arrival_date
                    ,clock_stop_event_datetime_utc
                    ,fulfillment_shipment_id
                    ,sum(quantity) as quantity
                from slam_data_transformed
                group by ordering_order_id
                    ,ship_day
                    ,ship_date_utc
                    ,ship_date_local
                    , asin
                    , int_dea_status 
                    ,warehouse_id
                    ,ship_method
                    ,internal_promised_delivery_date_item
                    ,estimated_arrival_date
                    ,clock_stop_event_datetime_utc
                    ,fulfillment_shipment_id
                    ,cast(order_datetime as date)
              ) dosi
            inner join 
             plans_df plans
            ON dosi.ORDER_ID = plans.ORDER_ID AND dosi.ASIN = plans.iaid
            where dosi.asin is not null
            and fta_planning_date is not null
            and lta_planning_date is not null
        """)
                                         ''',
                                                    lang=Lang.PYTHON,
                                                    GlueVersion="2.0",
                                                    WorkerType=GlueWorkerType.G_1X.value,
                                                    NumberOfWorkers=100,
                                                    Timeout=10 * 60
                                                )
                                            ])
    
    
    app.activate()
    
    # CREATE all of the datasets 
    app.process(daily_timer[datetime(2021, 1, 16)])
    # feed daily timer but target only 'denorm_archive_ad' (won't trigger dppi_df)
    # app.process(daily_timer[datetime(2021, 1, 16)], target_route_id=denorm_archive_ad)
    
    #app.execute(plans_df[datetime(2021, 1, 16)]Quick Experimentation with CDO Data (Andes) on Dev-Box and then Continuing in Sagemaker Notebook Instance

Step-by-step instructions using RheocerOS (targeting continuous experience on a Sagemaker notebook instance, moving data around from Andes into AWS compute, storages, intermediate dataset, etc):
 
_One-Time_ setup for BDT bridge, RheocerOS bundle (version-set, dependency-closure, etc) bridge into your notebook (synthesize a notebook instance using RheocerOS kernel).

*Target audience: Data scientists, BIE, DE, Engineers*


1. (5-10 mins) Grab an AWS account J and do this ONLY once (based on guideline from BDT): https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/ANDES_GLUE_SYNC_SETUP
2. (< 5mins) For each table do this once using the subscriber entity you will have in your account (use HOOT UI for now): https://code.amazon.com/packages/RheocerOS/blobs/5ffe85471b0fa77b3e1178eb9a9da9f3496a6545/--/new_app_template/ANDES_GLUE_SYNC_SETUP#L37
    1. (Recommended) Keep your hoot tab open (we will use table related information that in your RheocerOS code) or you will have to use the new Glue Data Catalog entry in the subsequent steps.
    2. We will use  BOOKER / D_UNIFIED_CUST_SHIPMENT_ITEMS in the example below:  https://hoot.corp.amazon.com/providers/booker/tables/D_UNIFIED_CUST_SHIPMENT_ITEMS/versions/14
3. Wait for sync to complete on your new subscription. Now you can start your exp in RheocerOS.
4. (5-10 mins, basic one-time Brazil Python stuff which to be fully automated soon) For only once, pull RheocerOS on your dev-box and **bootstrap** your notebook, differences for pulling on to Mac also indicated below (but using your dev-box is recommended for this bootstrapping):
    1. $ cd {YOUR_BRAZIL_WS_ROOT}    # example:  cd /workspace/yunusko
        $ brazil ws create --name IF_Core --versionset RheocerOS_Core/development
        $ cd IF_Core
        $ brazil ws use --package RheocerOS –branch mainline
        $ cd ./src/RheocerOS
        $ brazil ws use --platform AL2012
        # try the following command with single dash in case it fails
        # $  brazil ws use -platform AL2012 
    2. for linux:
        
        $ brazil setup platform-support --mode legacy
        
    3. For mac, check 
        
        $ brazil setup platform-support --help
        
        and see if "overlay" is still the default. Make sure that it is the default one by running it with that mode.
        
    4. $ brazil-build
        
        and finally bootstrap Python interpreter:
        
        $ brazil-bootstrap --environmentType runtime

1. (5 mins / Optional) Test your Andes based pipeline in AWS on your dev-box first. You need a logical/virtual app id (“lukasz-test”) and AWS region and account IDs.
    1. Create ‘examples/my_andes_pipeline.py’ for your new virtual app in AWS (ex: “lukasz-test”):
        1. Use this https://paste.amazon.com/show/yunusko/1616015494
    2. Since this particular example uses Midway credentials, do:
    3.     $ mwinit -o 
    4. Run via
        1. $ brazil-runtime-exec python examples/my_andes_pipeline.py
            
            
    5. You will see your Spark ETL being executed against DUCSI table and the path for the new partition in S3. This same experience is everywhere! You can run similar code against your application in a dev-box and also in a notebook
    6. If you want to run & debug in PyCharm, please refer https://code.amazon.com/packages/RheocerOS/blobs/e8b31b3073c1b119e7d91836a1811f4ec5a183bf/--/new_app_template/NEW_APP_STEPS#L208
2. (5-10 mins) Now you can provision different notebook instances for each application.
    1. In each notebook instance, you can have multiple notebooks using the same kernel doing ETL, accessing data within the sandbox of same application (“lukasz-test”). Permissions, IAM roles are all setup specific to this application).
    2. Modify  https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/examples/remote_notebook_provision.py
        1. Example modified one for this sample application (“lukasz-test”): https://paste.amazon.com/show/yunusko/1616015820
        2. $ brazil-runtime-exec python examples/remote_notebook_provision.py
3. (1-2 minutes) Now within your dev-box or alternatively using your **new notebook instance **you can build data-pipelines/flows from CDO side into dark recesses of AWS.
    1. Use the presigned-URL generated by step (6) above or go to AWS Console of your account and choose region ‘us-east-1’ (because I used that region).
    2. You will see your notebook instance ‘lukasz-test-notebook’ ACTIVE. Click on it and then from top-right corner, client ‘New’ and choose ‘RheocerOS’ as the kernel to create a new ‘notebook’.
    3. [Image: image]
4. In the new notebook, copy and paste this boilerplate in the first cell or partition it into multiple cells based on your taste: https://paste.amazon.com/show/yunusko/1616016101
    1. Run cells in sequential order (if your partitioned them already).
    2. Now you have a data-pipeline using Andes as a raw data source and running in AWS. This data-pipeline is _controlled_ from notebooks, dev-boxes and in production pipelines. It can be connected with other similar pipelines with two lines of code. Its datasets can be viewed and pulled into local python runtimes (or notebook cells) with zero insight into underlying infrastructure, storages, etc.
5. If you want to keep your data-pipeline running in the background and let it create new partitions based on Andes partition update events coming from BDT: then run the following API on your app object:
6. ***app.activate()
    
    ***
    1. Refer https://w.amazon.com/bin/view/DeliveryExperience/MachineLearning/RheocerOS/RheocerOSUserGuide/#HApplicationActivation

A snapshot from a Sagemaker notebook that analyzes/previews the result of an ETL against an Andes dataset:
[Image: sagemaker_intelliflow_kernel_preview.png]
**How to use different dependencies in ETL?** 

You might want to use dependencies such as other data-science libraries (scipy, etc) within your ETL. If they are already imported to your notebook instance, then RheocerOS will be able to deploy and use them in the target compute platforms. If there is an issue during the execution (like binary compatibility due to Python libraries with C extensions, etc) then it will let you know.

Another short-cut and more systematical way to declare your runtime that would even determine what would be ready for you on the target notebook instance is to use Brazil Config to pull new dependencies and go through step (6) above, which basically means re-syncing your remote notebook again. RheocerOS will update the runtime on your notebook and directly within the ETL or within the notebook cells you will be able to use new dependencies (Pandas, Numpy, or some other internal/third-party Brazil Python package):

```
brazil-runtime-exec python examples/remote_notebook_provision.py
```



### Andes Data Import as Spark Dataframe within Business Logic

If you don’t want to use Application::marshal_external_data and leverage Andes datasets as signals, and rather access them within your code to Application::create_data then you’d do something like this:


```
# DIRECT ACCESS into any Andes table (if table was sync'd into the Glue Data Catalog)
input_df = spark.sql(compact_andes(provider: str,
                                    table_name: str, 
                                   data_type: str,
                                   partition_keys: List[str],
                                   partitions: List[Tuple[Any]], # each tuple contains values for partitions. Example: [('NA', '2021-04-01'), ('NA', '2021-04-02')] 
                                   primary_keys: List[str]))


```

Examples:

```
# read table details from HOOT for example and update the parameters here.
d_addresses_df = spark.sql(compact_andes(provider="booker",
                      table_name="d_addresses", 
                      table_type="APPEND", 
                      partition_keys=[], 
                      all_partitions=[], 
                       primary_keys=["address_id"])).createOrReplaceTempView("d_addresses")

hydra_tosio_us_df: Dataset[Row] = spark.sql(compact_andes(provider="hydra-finance",
                        table_name="t_outbound_ship_items_ofa_us", 
                        table_type="REPLACE", 
                        partition_keys=["ship_month"], 
                        all_partitions=[["2021-04-01"]], 
                        primary_keys=["fulfillment_shipment_id", "customer_shipment_item_id"])).createOrReplaceTempView("t_outbound_ship_items_ofa_us")
```


Example in Application code:


```
daily_timer = app.add_timer("my_timer",
                            "rate(1 day)",
                            time_dimension_id="day")

daily_hydra = app.create("my_daily_hydra_data",
                         inputs=[daily_timer],
                         '''
                         current_day = dimensions["day"]  # what daily_timer "time" dimension value is.
                         
                         output = spark.sql(compact_andes(provider="hydra-finance",
                                            table_name="t_outbound_ship_items_ofa_us", 
                                            table_type="REPLACE", 
                                            partition_keys=["ship_month"], 
                                            all_partitions=[[f"{current_day}"]], 
                                            primary_keys=["fulfillment_shipment_id", "customer_shipment_item_id"]))
                         ''')
                         
# execute before waiting for daily timer to kick in
# waits/blocks and updates the user about the result of the execution
# once it is done, returns the full physical path of the output partition.
s3_path = app.execute(daily_hydra["2021-04-08"])

# output dataset is ready in s3_path
assert s3_path
```

### Andes data utilization in the scope of a production-ready data-pipelines in AWS.

* Create your own artifacts (Brazil package, etc): https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/new_app_template/NEW_APP_STEPS
* Paradigms:
    * Scheduled/conventional (direct access into Andes from within AWS Glue/EMR): https://code.amazon.com/packages/RheocerOSBlade/blobs/mainline/--/application/main.py
    * Event-driven: build a pipeline leveraging RheocerOS (signaling, partition management, dataframe extraction/compaction), extend this: https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/examples/external_andes_node_example.py
    * You use both in the same pipeline!
* If you want to directly access the datasets but want RheocerOS to setup BDT related connections, role setup, permission, then just **marshal** the data into the application (even if you are not going to use it as direct input to one of the create_data calls): 
        * Please refer https://code.amazon.com/packages/RheocerOSC2P/blobs/mainline/--/src/intelliflow_c2p/pipeline.py
        * This is the encapsulated marshaling call: https://code.amazon.com/packages/RheocerOSC2P/blobs/c0fc398e5257a1fd6a617330d2cf7bb0b74f9e2b/--/src/intelliflow_c2p/data_nodes/external/d_asin_marketplace_attributes/node.py#L5
        * Then in ETL, use it like: https://code.amazon.com/packages/RheocerOSC2P/blobs/c0fc398e5257a1fd6a617330d2cf7bb0b74f9e2b/--/src/intelliflow_c2p/data_nodes/internal/feature_engineering/tommy_data_with_digital/compute.py#L55

### Scala ETL

Please see this example from RheocerOS package to understand how you’d even mix Scala and Python (Spark) code within the same pipeline:

https://code.amazon.com/packages/RheocerOS/blobs/mainline/--/examples/mixed_scala_python_compute_example.py

The following example will run three parallel ETLs on the same Andes table and then pauses to avoid further auto-executions (triggers) in the background upon (partition update/create) events from BDT on the same table.

```
import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

flow.init_basic_logging()
flow.init_config()

app = AWSApplication("scala-python-mixed", "us-east-1", "842027028048")

# Add a CairnFS type table
d_ad_orders_na = app.marshal_external_data(
    AndesDataset("dex_ml_catalog", "d_ad_orders_na", partition_keys=["order_day"],
                 primary_keys=['customer_id'], table_type="REPLACE")
    , "d_ad_orders_na"
    , {
        'order_day': {
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d',   #  internally use this format
            'timezone': 'PST'
        }
    }
    , 
    {
        '*': {}
    }
)

repeat_d_ad_orders_na_scala = app.create_data(id="REPEAT_AD_ORDERS_IN_SCALA",
                                        inputs=[d_ad_orders_na],
                                        compute_targets=[
                                            BatchCompute(
                                                # Basic Spark Scala code encapsulated
                                                # can be multi line and import other modules
                                                scala_script("d_ad_orders_na.limit(100)"
                                                             #, external_library_paths=["s3://amazon-dexml-blade-beta/lib/DexmlBladeGlue-super.jar"]
                                                             ),
                                                lang=Lang.SCALA,
                                                GlueVersion="2.0",
                                                WorkerType=GlueWorkerType.G_1X.value,
                                                NumberOfWorkers=50,
                                                Timeout=3 * 60  # 3 hours
                                            )
                                        ])

repeat_d_ad_orders_na_python = app.create_data(id="REPEAT_AD_ORDERS_IN_PYTHON",
                                               inputs=[d_ad_orders_na],
                                               compute_targets=[
                                                   BatchCompute(
                                                       "output = d_ad_orders_na.limit(100)",
                                                       GlueVersion="2.0",
                                                       WorkerType=GlueWorkerType.G_1X.value,
                                                       NumberOfWorkers=50,
                                                       Timeout=3 * 60  # 3 hours
                                                   )
                                               ])


repeat_d_ad_orders_na_scala_complex = app.create_data(id="REPEAT_AD_ORDERS_IN_SCALA_COMPLEX",
                                              inputs=[d_ad_orders_na],
                                              compute_targets=[
                                                  BatchCompute(
                                                      scala_script("""import com.amazon.dexmlbladeglue.jobs.TrendFeaturesJob
                                                                 
                                                                    val order_day = dimensions("order_day")
                                                                    val my_param = args("my_param")
                                                                    // input dataframes are available both as local variables and also within
                                                                    // the spark context
                                                                    // Cradle type variables are also available (spark, inputs, inputTable, execParams)
                                                                    TrendFeatureWorker.main(args)
                                                                   """
                                                                   , external_library_paths=["s3://amazon-dexml-blade-beta/lib/DexmlBladeGlue-super.jar"]
                                                                   ),
                                                      lang=Lang.SCALA,
                                                      GlueVersion="2.0",
                                                      WorkerType=GlueWorkerType.G_1X.value,
                                                      NumberOfWorkers=50,
                                                      Timeout=3 * 60,  # 3 hours
                                                      my_param="PARAM1"
                                                  )
                                              ])

app.activate(allow_concurrent_executions=False)

# feed the activated AWS  app with a partition update on the source ANDES table
# three async executions will start in parallel on those three nodes above.
app.process(d_ad_orders_na["2021-01-14"])


# the following sequence can be intuitively resembled to 
# Thread::join operations in Threading APIs.
# blocking  wait on  first node (keeps user updated via console)
path, _ = app.poll(repeat_d_ad_orders_na_scala["2021-01-14"])
assert path
print(path)  # s3://if-scala-python-mixed-842027028048-us-east-1/internal_data/REPEAT_AD_ORDERS_IN_SCALA/2021-01-14

path, _ = app.poll(repeat_d_ad_orders_na_python["2021-01-14"])
assert path 

# stop listening events from BDT to avoid auto-trigger on above ETL nodes.
# save IMR
app.pause()
```