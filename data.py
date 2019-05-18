import asyncpg
import asyncio
from math import sqrt


async def get_db():
    """
    连接数据库
    :return: 数据库对象
    """
    db = await asyncpg.connect("postgresql://postgres:123456@127.0.0.1:5432/homework")
    return db


async def return_db(db: asyncpg.connection.Connection):
    await db.close()


async def get_all_users():
    db = await get_db()
    user_id_list = []
    users_sql = "SELECT id FROM \"user\""
    users = await db.fetch(
        users_sql
    )
    for user in users:
        user_id_list.append(user['id'])

    print("get all users", user_id_list)
    await return_db(db)
    return user_id_list


async def get_user_rate_sum(user_id):
    """
    获得用户评分和
    :return: 评分和: number
    """
    db = await get_db()
    sum_sql = "SELECT sum(rate) FROM bill WHERE user_id = " + str(user_id)
    sum_rate = await db.fetch(
        sum_sql
    )
    await return_db(db)
    print("get user_id:", user_id, "rate sum:", sum_rate[0]['sum'])
    return sum_rate[0]['sum']


async def get_user_squared_rate_sum(user_id):
    """
    获得用户评分方和
    :return: 评分方和: number
    """
    db = await get_db()
    squared_rate_sql = "SELECT sum(rate * rate) FROM bill WHERE user_id = " + str(user_id)
    squared_rate_sum = await db.fetch(
        squared_rate_sql
    )
    await return_db(db)
    print("get user_id:", user_id, "squared rate sum:", squared_rate_sum[0]['sum'])
    return squared_rate_sum[0]['sum']


async def get_both_rate_sum(user_1_id, user_2_id):
    rate_sum = 0
    for item in await get_both_bought(user_1_id, user_2_id):
        rate_sum += await get_user_item_avg_rate(user_1_id, str(item)) + await get_user_item_avg_rate(user_2_id,
                                                                                                      str(item))
    print("user_id:", user_1_id, "user_id:", user_2_id, "both rate sum:", rate_sum)
    return rate_sum


async def get_user_item_avg_rate(user_id, item_id):
    """
    获得用户对某物体的平均分
    :param user_id: string
    :param item_id: string
    :return: 平均分: number
    """
    db = await get_db()
    avg_rate_sql = "SELECT avg(rate) FROM bill WHERE user_id = " + str(user_id) + " AND item_id = " + str(item_id)
    rate = await db.fetch(
        avg_rate_sql
    )
    print(user_id, "rate items avg:", item_id, rate[0]['avg'])
    await return_db(db)
    return rate[0]['avg']


async def get_item_user_bought(user_id):
    """
    获得用户所有评价过的物体
    :param user_id: string
    :return: 物体集合: set(item_id: number)
    """
    db = await get_db()
    get_item_id = "SELECT DISTINCT item_id FROM bill WHERE user_id = " + str(user_id)
    item_set = set()
    items = await db.fetch(
        get_item_id
    )
    for item in items:
        item_set.add(item['item_id'])
    print(user_id, "rated", item_set)
    await return_db(db)
    return item_set


async def get_both_bought(user_1_id, user_2_id):
    """
    获得两个用户都买过的物体集合
    :param user_1_id:
    :param user_2_id:
    :return: 物体集合: set(item_id: number)
    """
    item_user_1_bought = await get_item_user_bought(user_1_id)
    item_user_2_bought = await get_item_user_bought(user_2_id)
    bought_set = item_user_1_bought & item_user_2_bought
    print(user_1_id, user_2_id, "both bought", bought_set)
    return bought_set


async def user_similarity_score(user_1_id, user_2_id):
    """
    获得两个用户的相似度分数
    :param user_1_id: string
    :param user_2_id: string
    :return: 用户相似度[0,1]
    """
    both_bought = await get_both_bought(user_1_id, user_2_id)
    if both_bought == set():
        print(user_1_id, user_2_id, "similarity score:", 0)
        return 0
    euclid_distance_list = list()
    for item_id in both_bought:
        user_1_rate = await get_user_item_avg_rate(user_1_id, str(item_id))
        user_2_rate = await get_user_item_avg_rate(user_2_id, str(item_id))
        euclid_distance_list.append(euclid_distance(user_1_rate, user_2_rate))
    sum_euclid_distance = sum(euclid_distance_list)
    score = 1 / (1 + sqrt(sum_euclid_distance))
    print("user_1_id:", user_1_id, "&user_2_id", user_2_id, "similarity score:", score)
    return score


async def user_correlation(user_1_id, user_2_id):
    """
    评价用户相关度
    :param user_1_id:
    :param user_2_id:
    :return: 用户相关度: number
    """
    both_bought_count = len(await get_both_bought(user_1_id, user_2_id))
    if both_bought_count == 0:
        return 0
    numerator = await get_both_rate_sum(user_1_id, user_2_id) - (
            await get_user_rate_sum(user_1_id) * await get_user_rate_sum(user_2_id)
            / both_bought_count
    )
    denominator = sqrt((await get_user_squared_rate_sum(user_1_id)
                        - pow(await get_user_rate_sum(user_1_id), 2))
                       * (await get_user_squared_rate_sum(user_2_id)
                          - pow(await get_user_rate_sum(user_2_id), 2)))
    if denominator == 0:
        return 0
    else:
        return numerator / denominator


async def get_similar_users(user_id):
    """
    获得最相似用户
    :param user_id:
    :return: 最相似用户id: string
    """
    all_users = await get_all_users()
    correlations = [(await user_correlation(user_id, user_id2), user_id2) for user_id2 in all_users if
                    user_id2 != user_id]
    correlations.sort()
    correlations.reverse()
    return correlations[:]


async def recommand(user_id):
    """
    给用户推荐最相似用户
    首先推荐 用户 没 有的 相似用户    有 的，增加数据量和用户满意度
    第二推荐 用户 没 有的 相似用户也 没有 的，增加数据量，防止用户过于相似（数据量够大时取消此操作）
    第三推荐 用户    有的，增加用户满意度
    :param user_id:
    :return:
    """
    db = await get_db()
    item_bought = await get_item_user_bought(user_id)
    similar_users = await get_similar_users(user_id)
    items_similar_users_bought = {}
    for user in similar_users:
        item_similar_user_bought = await get_item_user_bought(user[1])
        items_similar_users_bought[user] = item_similar_user_bought
    print(items_similar_users_bought)
    await return_db(db)


def euclid_distance(value1, value2):
    """
    欧几里得距离
    :param value1:
    :param value2:
    :return:
    """
    return pow(value1 - value2, 2)


async def get_all_bill():
    db = await get_db()
    sql = "SELECT * FROM bill"
    form = await db.fetch(sql)
    await return_db(db)
    print(form)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(get_item_user_bought('1'))
    asyncio.get_event_loop().run_until_complete(get_user_item_avg_rate('1', '1'))
    asyncio.get_event_loop().run_until_complete(get_all_bill())
    asyncio.get_event_loop().run_until_complete(user_similarity_score('1', '4'))
    asyncio.get_event_loop().run_until_complete(recommand('6'))
