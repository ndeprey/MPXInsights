import MySQLdb
import pandas as pd
import requests
import yaml
import time
import datetime

ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

pd.set_option('display.max_colwidth',100)

with open("/home/developer/leaderboards/config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

con = MySQLdb.connect(host=cfg['Cobalt']['host'],
                        passwd=cfg['Cobalt']['passwd'],
                        user=cfg['Cobalt']['user'],
                        db=cfg['Cobalt']['db']
                     )

sql1 = ('SELECT ratings_story_id, count(ratings_rating) as Tapthroughs '
	'FROM infinite.user_ratings '
	'WHERE ratings_rating in %(ratings)s '
	'AND ratings_origin in %(origins)s '
	'and ratings_timestamp >= Now() - interval "48" HOUR '
	'GROUP BY ratings_story_id '
	'ORDER By Tapthroughs desc '
	'limit 1000')

params1 = {"ratings": ['TAPTHRU'],
	"origins": ['FTRPROMO'] }

df = pd.read_sql(sql1, con=con, params=params1)

sql2 = ('SELECT ratings_story_id, count(ratings_rating) as totalImpressions '
        'FROM infinite.user_ratings '
        'WHERE ratings_rating in %(ratings)s '
        'AND ratings_origin in %(origins)s '
        'and ratings_timestamp >= Now() - interval "48" HOUR '
        'GROUP BY ratings_story_id '
        'ORDER By totalImpressions desc '
        'limit 1000')

params2 = {"ratings": ['THUMBUP','SHARE','COMPLETED','SKIP','START','TAPTHRU'],
        "origins": ['FTRPROMO'] }

df2 = pd.read_sql(sql2, con=con, params=params2)

df = pd.merge(df, df2,
	    left_on = ['ratings_story_id'],
	    right_on = ['ratings_story_id'],
	    how = 'inner')

df['Tapthrough_rate'] = df['Tapthroughs'] / df['totalImpressions']

df['Tapthrough_rate'] = [100*round(i,4) for i in df['Tapthrough_rate']]

# df = df[df['totalImpressions'] >= 1000]

df = df.sort(['Tapthrough_rate'], ascending=False)

print 'loaded results from SQL query with rows = ', len(df)
con.close()

print 'Done'
    
print df.shape


def get_title(id):
	baseurl = "http://api.npr.org/query"
	payload = {'id':id,
	'fields': 'title,teaser',
	'output':'JSON',
	'apiKey':'MDE1MTM2Mjk1MDE0MDUwMTIwNzlmYTdkNA001'}
	r = requests.get(baseurl, params=payload)
	r = r.json()
	try:
		title = r['list']['story'][0]['title']['$text']
	except:
		title = '(no title found)'
	return title

df['title'] = [get_title(id) for id in df['ratings_story_id']]

print 'done fetching titles'

df.to_csv('/var/www/promo_leaderboard.csv', encoding='utf-8', index=False)

df = df[0:10]

df['link_story_id'] = ['<a href="http://npr.org/' + str(int(i)) + '">' + str(int(i)) + '</a>' for i in df['ratings_story_id']]

print 'Script completed'

df = df[['link_story_id','title','totalImpressions','Tapthrough_rate']]

dfhtml = df.to_html(index=False,escape=False).replace('<table border="1" class="dataframe">','<table class="table table-striped">') # use bootstrap styling

html_string = '''
<html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
	<meta http-equiv="refresh" content="300" >
    </head>
    <body>
	 <h1>NPR One Promo Leaderboard</h1>
	 <h5>Last Updated: '''+ st + '''</h5>
	  <p>A list of the 10 promos with the highest tapthrough rates in NPR One over the last 48 hours.</p>
''' + dfhtml + '''
    </body>
</html>'''

f = open('/var/www/promo_leaderboard.html','w')
f.write(html_string)
f.close()

print 'done saving HTML table'




