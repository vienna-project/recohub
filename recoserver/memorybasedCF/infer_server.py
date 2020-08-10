"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""


"""
reference : 
* [Is Sanic python web framework the new Flask?](https://medium.com/@ahmed.nafies/is-sanic-python-web-framework-the-new-flask-2fe06b409fa3)


"""
from sanic import Sanic
from sanic import response as res
from sanic import request as req
from sanic_redis import SanicRedis
from collections import Counter

try:
    from minhash import decompress_sigindex
except ImportError:
    from .minhash import decompress_sigindex

app = Sanic(__name__)
app.config.update(
    {'REDIS': {"address": ('redis', 6379)}}
)
redis = SanicRedis(app)


@app.route('/repository', methods=["GET"])
async def get_repository_recommendation(request: req):
    global redis
    repo_id = request.args.get('repo_id', None)
    num_recommend = request.args.get("num_recommend", 10)

    if isinstance(repo_id, str) and repo_id.isnumeric():
        repo_id = int(repo_id)
    else:
        return res.text("No repo_id", status=404)

    with await redis.conn as r:
        sigs = await r.get(repo_id)

    if sigs:
        sigs = decompress_sigindex(sigs)
    else:
        return res.text(f"Not Found (repo_id : {repo_id})", status=404)

    with await redis.conn as r:
        values = await r.mget(*[f"sig{i}-{sig}" for i, sig in enumerate(sigs)])

    repo_counts = Counter(
        [repo_id for value in values for repo_id in decompress_sigindex(value)])

    recommend_repos = sorted(repo_counts,
                             key=lambda x: repo_counts[x],
                             reverse=True)[1:num_recommend+1]

    return res.json({"repository": recommend_repos})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)

