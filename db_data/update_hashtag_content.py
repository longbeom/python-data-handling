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


# 해시태그풀 조회, 해시태그 딕셔너리 생성
def get_hashtag_pool():
    sql = "SELECT id, hashtag FROM hashtag_pool"
    try:
        cursor.execute(sql)
        hashtag_pool = cursor.fetchall()
    except Exception as ex:
        print('get hashtag_pool error', ex)

    hashtags = {}
    for hashtag in hashtag_pool:
        hashtag_id = hashtag[0]
        hashtag_name = hashtag[1]
        hashtags[hashtag_name] = {}
        hashtags[hashtag_name]['id'] = hashtag_id
        hashtags[hashtag_name]['guide_ids'] = set()
        hashtags[hashtag_name]['product_ids'] = set()

    return hashtags


# 콘텐츠의 해시태그 조회
def get_hashtag_content(content):
    sql = "SELECT idx, hash_tag_one, hash_tag_two, hash_tag_thr FROM %s" % content

    try:
        cursor.execute(sql)
    except Exception as ex:
        print('get hashtag_content from %s' % content, ex)
    return cursor.fetchall()


# 해시태그 딕셔너리에 해당하는 콘텐츠 id 저장
def integration_hashtag_pool_content():
    hashtags = get_hashtag_pool()
    for hashtag_content in get_hashtag_content('content'):
        guide_id = hashtag_content[0]
        hashtags[hashtag_content[1]]['guide_ids'].add(guide_id)
        hashtags[hashtag_content[2]]['guide_ids'].add(guide_id)
        hashtags[hashtag_content[3]]['guide_ids'].add(guide_id)
    for hashtag_content in get_hashtag_content('product'):
        product_id = hashtag_content[0]
        hashtags[hashtag_content[1]]['product_ids'].add(product_id)
        hashtags[hashtag_content[2]]['product_ids'].add(product_id)
        hashtags[hashtag_content[3]]['product_ids'].add(product_id)
    return hashtags


# 해시태그 연관 콘텐츠 id 저장
def update_hashtag_content():
    hashtag_guide_product = [i[0] for i in get_hashtag_guide_product()]
    hashtags = integration_hashtag_pool_content()
    for hashtag in hashtags:
        hashtag_id = hashtags[hashtag]['id']
        guide_ids = '{}' if hashtags[hashtag]['guide_ids'] == set() else hashtags[hashtag]['guide_ids']
        product_ids = '{}' if hashtags[hashtag]['product_ids'] == set() else hashtags[hashtag]['product_ids']

        if hashtag_id in hashtag_guide_product:
            sql = "UPDATE hashtag_guide_product SET guide_ids = '%s', product_ids = '%s' WHERE hashtag_id = %d" \
                  % (guide_ids, product_ids, hashtag_id)
            cursor.execute(sql)
        else:
            sql = "INSERT INTO hashtag_guide_product VALUES(%d, '%s', '%s')" % (hashtag_id, guide_ids, product_ids)
            cursor.execute(sql)

        print(hashtag_id, guide_ids, product_ids)
    try:
        conn.commit()
    except Exception as ex:
        conn.rollback()
        print('save hashtag_guide_product error!', ex)


# 기존에 저장된 해시태그 연관 콘텐츠 조회
def get_hashtag_guide_product():
    sql = "SELECT hashtag_id FROM hashtag_guide_product"
    cursor.execute(sql)
    return cursor.fetchall()


update_hashtag_content()
