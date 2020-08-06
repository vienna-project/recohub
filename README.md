# Recohub : Finding github repository you should know (on going project)

### Objective

> In more than 100 million repositories, there are hidden awesome repositories that developers need to know. Through Github's public events, we are developing services that identify the characteristics of developers and find the optimal repository

### Architecture

![](https://imgur.com/fuarWwN.png)


### DataSource 

* [gharchive](https://www.gharchive.org/)
    
    Github provides Data related to public events to BigQuery every day. BigQuery allows you to analyze various interaction information between developers and repositories. 

* [github API](https://docs.github.com/en/graphql)
 
   There is no metadata about developers and repositories in the information provided in BigQuery. We can get the metadata through the Github API. 
    

### Progress

##### 1. Analyze Data EDA (in progress)

   links to [github Anyalsis](https://github.com/vienna-project/github-analysis).

##### 2. Construct Crawling Pipeline (in progress)



##### 3. Construct Recommendation System (to do)



##### 4. Construct Recohub Service (to do)