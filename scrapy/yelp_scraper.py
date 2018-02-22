# -*- coding: utf-8 -*-
#Note: The following URLS (apart from Northfield) have not been run yet: we ran out of crawlera :(

import scrapy
import logging

from selenium import webdriver
from datetime import datetime
from scrapy.http.request import Request

class RestaurantSpider(scrapy.Spider):
    name = "yelp"
    allowed_domains = ['www.yelp.com']
    
    def start_requests(self):
        if self.settings['PLACE'] == 'MINNEAPOLIS':
            place = ["minneapolis"]
            urls = [
                   'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Bryn_Mawr',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Downtown_Minneapolis',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Linden_Hills',
                   'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Longfellow',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Loring_Park',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Near_North',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Nicollet_Island',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Nokomis',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::North',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::North_Loop',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:MN:Minneapolis::Northeast'
                   ]
            for url in urls:
                yield Request(url, self.parse, meta = {'place' : place})
                
        elif self.settings['PLACE'] == 'SEATTLE':
            place = ["seattle"]
            urls = ['https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Admiral',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Alki',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Atlantic',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Ballard',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Beacon_Hill',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Belltown',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Bitter_Lake',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Brighton',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Broadview',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Bryant',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Capitol_Hill',
                    'https://www.yelp.com/search?find_desc=restaurants&start=0&l=p:WA:Seattle::Central_District'
            ]
            for url in urls:
                yield Request(url, self.parse, meta = {'place' : place})
                
        elif self.settings['PLACE'] == 'HOUSTON':
            place = ['houston']
            urls = ['https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Galleria/Uptown',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Golfcrest/Belfort/Reveille',
                   'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Greenspoint',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Greenway',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Gulfgate/Pine_Valley',
                    'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Gulfton',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Harrisburg/Manchester',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Hidden_Valley',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Highland_Village',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Hobby',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::IAH_Airport_Area',
                'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Independence_Heights',
               'https://www.yelp.com/search?find_desc=Restaurants&start=0&l=p:TX:Houston::Inwood'
            ]
            for url in urls:
                yield Request(url, self.parse, meta={'place' : place})
                
        else:
            place = ["northfield"]
            yield Request('https://www.yelp.com/search?find_desc=restaurants&find_loc=Northfield,+MN+55057', self.parse, meta={'place' : place})
            
    def parse(self, response):
        place = response.meta.get('place')[0]
        for rest in response.css('.indexed-biz-name'):
            restaurant_url = rest.css('.biz-name.js-analytics-click::attr(href)').extract_first()
            data = [restaurant_url, place]
            yield response.follow(restaurant_url, callback = self.parse_restaurant, meta={'data': data})
            
        next_page = response.css('.u-decoration-none.next.pagination-links_anchor::attr(href)').extract_first()
        if next_page is not None:
            data = [place]
            yield response.follow(next_page, callback=self.parse, meta={'place': data})
            
    def parse_restaurant(self,response):
        data = response.meta.get('data')
        rest_url = data[0]
        place = data[1]
        rest_id = response.css('.media-title .biz-name::attr(data-hovercard-id)').extract_first()
        name = response.css('.biz-page-title.embossed-text-white::text').extract_first()
        if name is not None:
            name = name.strip()
        else:
            name = response.css('.ysection.questions > p > strong::text').extract_first()
        price_range = response.css('.business-attribute.price-range::text').extract_first()
        if price_range is not None:
            price_range = len(price_range)
        else:
            price_range = 0
        cuisines = response.css('.top-shelf .price-category span a::text').extract()
        cuisines = ", ".join(cuisines)
        location = response.css('.hidden address span::text').extract()
        location = " ".join(location)
        avg_rating = response.css('.rating-info .rating-very-large::attr(title)').extract_first()
        if avg_rating is not None:
            avg_rating = avg_rating[0:3]
        num_ratings = response.css('.rating-info .review-count.rating-qualifier::text').extract_first()
        if num_ratings is None:
            num_ratings = 0
        elif num_ratings == '':
            num_ratings = 0
        else:
            num_ratings = num_ratings.strip()
            if len(num_ratings) == 8:
                num_ratings = num_ratings[0]
            else:
                num_ratings = num_ratings[:-8]
        data_points = []
        data_points.append(rest_id)
        data_points.append(place)
        
        yield {
            'type': 'restaurant',
            'scrapy_id' : place,
            'rest_url' : rest_url,
            'site': 'yelp',
            'name' : name,
            'rest_id' : rest_id,
            'avg_rating' : avg_rating,
            'num_ratings' : num_ratings,
            'price_range' : price_range,
            'cuisines' : cuisines,
            'location' : location,
        }

        next_page = response.css('.location .media-title .biz-name.js-analytics-click::attr(href)').extract_first()
        if next_page is not None:
            yield response.follow(next_page, callback = self.parse_review_page, meta={'data': data_points})

        next_photo_url = response.css('.js-photo.photo.photo-2 .showcase-photo-box a::attr(href)').extract_first()
        photo_urls = []
        data_points.append(photo_urls)
        if next_photo_url is not None:
            yield response.follow(next_photo_url, callback = self.parse_photos, meta={'data' : data_points})
        else:
            next_photo_url = response.css('.js-photo.photo.photo-1 .showcase-photo-box a::attr(href)').extract_first()
            if next_photo_url is not None:
                yield response.follow(next_photo_url, callback = self.parse_photos, meta = {'data' : data_points})
        
    def parse_photos(self,response):
        data = response.meta.get('data')
        rest_id = data[0]
        place = data[1]
        image_id = response.css('.media.js-media-photo::attr(data-photo-id)').extract_first()
        image_url = response.css('.photo-box-img::attr(src)').extract_first()
        photo_date = response.css('.selected-photo-upload-date.time-stamp::text').extract_first()
        if photo_date is not None:
            photo_date = datetime.strptime(photo_date,'%B %d, %Y')
        else:
            photo_date = datetime.strptime('01/01/1900','%m/%d/%Y')
        user_id = response.css('.user-display-name.js-analytics-click::attr(data-hovercard-id)').extract_first()
        if user_id is None:
            user_id = response.css('.biz-name.js-analytics-click::attr(data-hovercard-id)').extract_first()
        yield {
            'type' : 'photo',
            'scrapy_id' : place,
            'site' : 'yelp',
            'rest_id' : rest_id,
            'image_id' : image_id,
            'user_id' : user_id,
            'photo_date' : photo_date,
            'image_url' : image_url,
        }
        
        next_arrow = response.css('.media-nav_link.media-nav_link--next.js-media-nav_link--next::attr(href)').extract_first()
        if next_arrow is not None:
            yield response.follow(next_arrow, callback = self.parse_photos, meta = {'data':data})
    
    def parse_review_page(self,response):
        data = response.meta.get('data')
        rest_id = data[0]
        place = data[1]
        
        for review in response.css('.review.review--with-sidebar'):
            review_id = review.css('.review.review--with-sidebar::attr(data-review-id)').extract_first()
            if review_id is not None:
                reviewer_location = review.css('.user-location.responsive-hidden-small > b::text').extract_first()
                photo_urls = review.css('.photo-box-grid.clearfix.js-content-expandable.lightbox-media-parent img::attr(data-async-src)').extract()
                if photo_urls is not None:
                    photo_urls = ", ".join(photo_urls)
                user_id = review.css('.user-name > a::attr(data-hovercard-id)').extract_first()
                review_content = ""
                for content in review.css('.review-content > p'): 
                    review_content = content.css('::text').extract() 
                review_content = " ".join(review_content)
                review_rating = review.css('.biz-rating.biz-rating-large.clearfix div div::attr(title)').extract_first()
                if review_rating is not None:
                    review_rating = review_rating[0:3]
                review_date_old = review.css('.biz-rating.biz-rating-large.clearfix .rating-qualifier::text').extract_first()
                if review_date_old is not None:
                    review_date_old = review_date_old.strip()
                    review_date = datetime.strptime(review_date_old, '%m/%d/%Y')
                else:
                    review_date = datetime.strptime('01/01/1900', '%m/%d/%Y')
                useful = review.css('.voting-buttons .ybtn.ybtn--small.useful.js-analytics-click .count::text').extract_first()
                funny = review.css('.voting-buttons .ybtn.ybtn--small.funny.js-analytics-click .count::text').extract_first()
                cool = review.css('.voting-buttons .ybtn.ybtn--small.cool.js-analytics-click .count::text').extract_first()
                if useful is None:
                    useful = 0
                if funny is None:
                    funny = 0
                if cool is None:
                    cool = 0
                votes_tuple = str(useful) + ", " + str(funny) + ", " + str(cool)
                yield {
                    'type' : 'review',
                    'scrapy_id' : place,
                    'site' : 'yelp',
                    'rest_id' : rest_id,
                    'review_id' : review_id,
                    'user_id' : user_id,
                    'reviewer_location' : reviewer_location,
                    'photo_urls' : photo_urls,
                    'review_content' : review_content,
                    'review_rating' : review_rating,
                    'review_date' : review_date,
                    'yelp_votes' : votes_tuple,
                    'four_votes' : None,
                }

        next_review_page = response.css('.u-decoration-none.next.pagination-links_anchor::attr(href)').extract_first()
        if next_review_page is not None:
            yield response.follow(next_review_page, callback=self.parse_review_page,meta={'data': data})
            
if __name__ == "__main__":
    main()
