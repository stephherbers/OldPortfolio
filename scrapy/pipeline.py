
import logging
import MySQLdb
from datetime import datetime

class RestaurantbotPipeline(object):
    
    def open_spider(self, spider):
        self.db = MySQLdb.connect(user="grplens", passwd="atth1132", db="restaurants", use_unicode=True, charset="utf8mb4")

    def close_spider(self, spider):
        self.db.close()
    
    def process_item(self, item, spider):
        if item["type"] == "restaurant":
            self.store_restaurant(item)
        elif item["type"] == "review":
            self.store_review(item)
        elif item["type"] == "photo":
            self.store_photo(item)
        return item
    
    def store_restaurant(self, item):
        sql = """
            INSERT INTO restaurant 
            (id, scrapy_id, site, name, url, num_ratings, avg_rating, price_range, cuisines, location) 
            VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            name=%s, url=%s, num_ratings=%s, avg_rating=%s, price_range=%s, cuisines=%s, location=%s;
        """
        c = self.db.cursor()
        c.execute(sql, (
            item['rest_id'], 
            item['scrapy_id'],
            item['site'], 
            item['name'],
            item['rest_url'],
            item['num_ratings'],
            item['avg_rating'],
            item['price_range'],
            item['cuisines'],
            item['location'],
            item['name'],
            item['rest_url'],
            item['num_ratings'],
            item['avg_rating'],
            item['price_range'],
            item['cuisines'],
            item['location']
        ))
        self.db.commit()
    
    def store_review(self,item):
        sql = """
            INSERT INTO review
            (id, scrapy_id, site, rest_id, user_id, user_location, review_content, review_rating, yelp_votes, four_votes, review_date, photo_urls)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE user_location=%s, review_content=%s, review_rating=%s, yelp_votes=%s, four_votes=%s, review_date=%s, photo_urls=%s;
        """
        
        c = self.db.cursor()
        try:       
            c.execute(sql, (
                item['review_id'],
                item['scrapy_id'],
                item['site'],
                item['rest_id'],
                item['user_id'],
                item['reviewer_location'],
                item['review_content'],
                item['review_rating'],
                item['yelp_votes'],
                item['four_votes'],
                datetime.strftime(item['review_date'],'%Y-%m-%d %H:%M:%S'),
                item['photo_urls'],
                item['reviewer_location'],
                item['review_content'],
                item['review_rating'],
                item['yelp_votes'],
                item['four_votes'],
                datetime.strftime(item['review_date'],'%Y-%m-%d %H:%M:%S'),
                item['photo_urls']
            ))
            self.db.commit()
        except:    
            raise
        
    def store_photo(self, item):
        sql = """
            INSERT INTO photo
            (id, scrapy_id, site, rest_id, user_id, photo_date, url)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            rest_id=%s, user_id=%s, photo_date=%s, url=%s;
        """
        
        c = self.db.cursor()
        c.execute(sql, (
            item['image_id'],
            item['scrapy_id'],
            item['site'],
            item['rest_id'],
            item['user_id'],
            datetime.strftime(item['photo_date'],'%Y-%m-%d %H:%M:%S'),
            item['image_url'],
            item['rest_id'],
            item['user_id'],
            datetime.strftime(item['photo_date'],'%Y-%m-%d %H:%M:%S'),
            item['image_url']
        ))
        self.db.commit()
