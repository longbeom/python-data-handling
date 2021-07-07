import sys

import psycopg2

from config import Config

try:
    conn = psycopg2.connect(host=Config.DB_HOST_DEBUG, dbname=Config.DB_NAME, user=Config.DB_USER,
                            password=Config.DB_PASSWORD)
    cursor = conn.cursor()
except Exception as e:
    print('db connection failed, %s' % e)
    sys.exit()


# 기존 풀에 없으면 저장
def hashtag_update():
    all_hashtags = get_hashtags('content').union(get_hashtags('product'))  # hashtags of guides + hashtags of products
    hashtag_pool = [i[0] for i in get_hashtag_pool()]  # convert list of tuple to list

    for hashtag in all_hashtags:
        if hashtag not in hashtag_pool:
            save_hashtag(hashtag)

    try:
        conn.commit()
    except Exception as ex:
        conn.rolback()
        print('error - rollback', ex)


# 콘텐츠에서 해시태그 가져옴
def get_hashtags(content):
    hashtag_set = set()
    sql = "SELECT hash_tag_one, hash_tag_two, hash_tag_thr FROM %s" % content

    try:
        cursor.execute(sql)
        for hashtags in cursor.fetchall():
            tag1 = hashtags[0]
            tag2 = hashtags[1]
            tag3 = hashtags[2]
            hashtag_set.update([tag1, tag2, tag3])
    except Exception as ex:
        print('get hashtag from %s' % content, ex)
    return hashtag_set


# 기존 풀의 해시태그들 조회
def get_hashtag_pool():
    sql = "SELECT hashtag FROM hashtag_pool"
    try:
        cursor.execute(sql)
        hashtag_pool = cursor.fetchall()
    except Exception as ex:
        print('get hashtag_pool error', ex)
    return hashtag_pool


# 해시태그 저장
def save_hashtag(hashtag):
    sql = "INSERT INTO hashtag_pool (hashtag) VALUES ('%s')" % hashtag
    try:
        cursor.execute(sql)
    except Exception as ex:
        print('save hashtag_pool error, hashtag: %s' % hashtag, ex)


hashtag_update()
