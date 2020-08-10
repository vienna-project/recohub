"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""


"""
reference : 
* [Is Sanic python web framework the new Flask?](https://medium.com/@ahmed.nafies/is-sanic-python-web-framework-the-new-flask-2fe06b409fa3)


"""
import numpy as np
from sanic import Sanic
from sanic import response as res
from sanic import request as req
from sanic_redis import SanicRedis
from collections import Counter

try:
    from minhash import (generate_minhash, generate_sigindex, get_permutation,
                          compress_sigindex, decompress_sigindex, sigmap_for_append,
                          sigmap_for_remove, extract_sigatures_to_update)
except ImportError:
    from .minhash import (generate_minhash, generate_sigindex, get_permutation,
                          compress_sigindex, decompress_sigindex, sigmap_for_append,
                          sigmap_for_remove, extract_sigatures_to_update)

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


@app.route('/repository', methods=["PUT", "POST"])
async def update_repository_recommendation(request: req):
    global redis
    repo_id = request.args.get("repo_id", None)
    if isinstance(repo_id, str) and repo_id.isnumeric():
        repo_id = str(repo_id)

    else:
        return res.text("No repo_id", status=404)

    user_id = request.args.get("user_id", None)
    if isinstance(repo_id, str) and repo_id.isnumeric():
        user_minhash = generate_minhash(user_id)
    else:
        return res.text("No user_id", status=404)

    with await redis.conn as r:
        # TODO : Transaction으로 구현해야 함
        mapping = {}
        deletes = []

        item_minhash = await r.get(repo_id)
        if item_minhash:
            item_minhash = np.array(decompress_sigindex(item_minhash))
        if item_minhash is None:
            # item의 이전 signature 정보가 없으므로 User Minhash를 모두 저장
            item_minhash = user_minhash
            sigs = generate_sigindex(user_minhash)
            sig_values = await r.mget(sigs)
            mapping = sigmap_for_append(sigs, sig_values, repo_id)
        else:
            # repository의 minhash와 user_minhash를 비교한 후, secondary index 중
            # - mh_remove : signature 중 repo_id를 제거해야 하는 signature 목록
            # - mh_append : signature 중 repo_id를 추가해야 하는 signature 목록
            sigs, mh_remove, mh_append = extract_sigatures_to_update(item_minhash, user_minhash)
            ra_sigs = generate_sigindex(mh_remove, sigs) + generate_sigindex(mh_append, sigs)
            if not ra_sigs:
                return res.text("Success", status=201)
            ra_sig_values = await r.mget(ra_sigs)

            nums = len(sigs)
            mapping, deletes = sigmap_for_remove(ra_sigs[:nums], ra_sig_values[:nums], repo_id)
            mapping.update(sigmap_for_append(ra_sigs[nums:], ra_sig_values[nums:], repo_id))

        if deletes:
            await r.delete(*deletes)

        minhash = np.minimum(item_minhash, user_minhash)
        mapping[repo_id] = compress_sigindex(minhash.tolist())
        await r.mset(mapping)

    return res.text("Success", status=201)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)

