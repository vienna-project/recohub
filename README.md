# Recohub : Finding github repository you should know (on going project)

### Objective

> In more than 100 million repositories, there are hidden awesome repositories that developers need to know. Through Github's public events, we are developing services that identify the characteristics of developers and find the optimal repository

### Usage
Super easy. Just Code it below. server will be launched.

````bash
docker-compose up -d
````

ports : 
* `27017`: mongoDB
* `6379`: redis
* `8080`: redis-stats
* `8081`: mongo-express

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

   Currently, only Crawl Repository Metadata.

##### 3. Construct Recommendation System (in progress)

   see `dev-recommendation` branch. 

##### 4. Construct Recohub Service (to do)