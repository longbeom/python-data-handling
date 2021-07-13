import sys
from datetime import datetime
from datetime import timedelta

import psycopg2
import csv
from config import Config

try:
    conn = psycopg2.connect(host=Config.DB_HOST_DEBUG, dbname=Config.DB_NAME, user=Config.DB_USER,
                            password=Config.DB_PASSWORD)
    cursor = conn.cursor()
except Exception as e:
    print('db connection failed, %s' % e)
    sys.exit()


def review_count_between_date(start, end):
    filename = 'review_count_%s_%s.csv' % (start, end)
    file = open(filename, 'w', newline='')
    wr = csv.writer(file)

    reviews = get_reviews()

    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')

    diff_days = (end_date - start_date).days + 1

    for i in range(diff_days):
        date = (start_date + timedelta(days=i)).date()

        try:
            count = reviews[date]
        except KeyError:
            count = 0
        print('%s, %d' % (date, count))
        wr.writerow([date, count])


def get_reviews():
    sql = "SELECT DATE(created_at), COUNT(*) FROM product_review WHERE is_valid IS TRUE GROUP BY DATE(created_at)"
    cursor.execute(sql)
    reviews = {}
    for review in cursor.fetchall():
        date = review[0]
        count = review[1]
        reviews[date] = count
    return reviews


review_count_between_date('2021-04-28', '2021-07-12')
