
<!--

Provide a short summary in the Title above. Examples of good PR titles:

* "feature/add fct_order and dim_customer models"

* "fix/deduplicate dim_customer"

* "test/esnure order-customer ref integrity"

NOTE: Pull Requests will be labled automatically using GitHub actions with the label feature, fix or test if the branch naming strategy is being followed. Optionally you can add the label manually. Using these labels ensures the release notes are generated when the code is promoted to Production.

-->

## Change Request documentation

<!--

Ensure the change request documentation is completed PRIOR to merging changes into UAT and PRODUCTION:

https://advocatehealth.sharepoint.com/sites/GRP-MDP-Cloud-Data-Engineering/Shared%20Documents/Forms/AllItems.aspx?csf=1&web=1&e=pnSzxF&cid=48d2b7fd%2D9114%2D4ec5%2Da897%2D59b29dad66d8&RootFolder=%2Fsites%2FGRP%2DMDP%2DCloud%2DData%2DEngineering%2FShared%20Documents%2FDeveloper%20Team%20Artifacts%2FDesign%20Reviews%20%2D%20Mandatory%20%2D%20PROD%2DUAT%2FChange%20Control&FolderCTID=0x012000DECB0F9AB0DE384084B68850E1C82B54

-->

## Description

<!--

Describe your changes, and why you're making them. Is this linked to an open

issue, ADO task, or another pull request? Link it here.

-->


## To-do before merge

<!--

(Optional -- remove this section if not needed)

Include any notes about things that need to happen before this PR is merged, e.g.:

- [ ] Change the base branch

- [ ] Update dbt Cloud jobs

- [ ] Ensure PR #56 is merged

-->

## Screenshots:

<!--

Include a screenshot of the relevant section of the updated DAG. You can access

your version of the DAG by running `dbt docs generate && dbt docs serve`.

-->

## Validation of models:

<!--

Include any output that confirms that the models do what is expected. This might

be a link to an in-development dashboard in your BI tool, or a query that

compares an existing model with a new one.

-->

## Changes to existing models:

<!--

Include this section if you are changing any existing models. Link any related

pull requests, or instructions for merge (e.g. whether old

models should be dropped after merge, or whether a full-refresh run is required)

-->

## Checklist:

<!--

This checklist is mostly useful as a reminder of small things that can easily be

forgotten – it is meant as a helpful tool rather than hoops to jump through.

Put an `x` in all the items that apply, make notes next to any that haven't been

addressed, and remove any items that are not relevant to this PR.

-->

- [ ] I have created a Change Request document in the appropriate SharePoint location.

- [ ] My pull request represents one logical piece of work.

- [ ] My commits are related to the pull request and look clean.

- [ ] I have materialized my models appropriately.

- [ ] I unit tested my models appropriately.

- [ ] I have added appropriate tests and documentation to any new models.

- [ ] I have updated the README file (if necessary).