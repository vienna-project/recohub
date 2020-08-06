"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
GETLIMIT_QUERY = '''
query {
  rateLimit(dryRun:true) {
    limit,
    cost,
    remaining,
    resetAt
  }  
}

'''


GETREPO_QUERY = """
query GetRepo($owner: String!, $name: String!) { 
  repository(owner:$owner, name:$name) {
    id, 
    name,
	  owner {
      login
    },
    homepageUrl,
    createdAt,
    updatedAt,  
    pushedAt,
    description,
    diskUsage,
    forkCount,
    hasWikiEnabled,
    hasIssuesEnabled,
    hasProjectsEnabled,
    isFork,    
    isArchived,
    isDisabled,
    isEmpty,
    isFork,
    isLocked,
    isMirror,
    isPrivate,
    isTemplate,
    mergeCommitAllowed,

    watchers(first:1){
      totalCount
    },

    stargazers(first:1){
      totalCount
    },

    commitComments(first:1){
      totalCount
    },

    pullRequests {
      totalCount
    },

    releases(first:1) {
      totalCount
    },

    primaryLanguage {
      name
    },

    languages(first:100) {
      totalCount,
      nodes {
        name
      }
    },

    labels(first:1) {
      totalCount
    },

    licenseInfo {
      name,
      nickname
    },

    deployments {
      totalCount
    },

    repositoryTopics(first:100){
      totalCount,
      nodes {
        topic{
          name
        }
      }
    },    
  },
  rateLimit {
    limit,
    cost,
    remaining,
    resetAt
  }  
}
"""
