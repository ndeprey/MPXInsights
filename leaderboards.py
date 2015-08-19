import MySQLdb
import pandas as pd
import requests

con = MySQLdb.connect('stage4-mysql.npr.org',
		      'admin',
		      'admin',
		      'infinite')

sql = ('SELECT ratings_story_id, count(ratings_rating) as ThumbupsAndShares '
	'FROM infinite.user_ratings '
	'WHERE ratings_rating in %(ratings)s '
	'AND ratings_origin in %(origins)s '
	'and ratings_timestamp >= Now() - interval "24" HOUR '
	'GROUP BY ratings_story_id '
	'ORDER By ThumbupsAndShares desc '
	'limit 1000')

params = {"ratings": ['THUMBUP','SHARE'],
	"origins": ['LEAD','CORE','BREAK','SELECTS','INVEST','EDTR','MUSICINT','OPENDOOR'] }

df = pd.read_sql(sql, con=con, params=params)

print 'loaded results from SQL query with rows = ', len(df)
con.close()

print 'Done'
    
print df.shape

df = df[0:10]

def get_title(id):
	baseurl = "http://api.npr.org/query"
	payload = {'id':id,
	'fields': 'title,teaser',
	'output':'JSON',
	'apiKey':'MDE1MTM2Mjk1MDE0MDUwMTIwNzlmYTdkNA001'}
	r = requests.get(baseurl, params=payload)
	r = r.json()
	title = r['list']['story'][0]['title']['$text']
	return title

df['title'] = [get_title(id) for id in df['ratings_story_id']]

print 'done fetching titles'

df.to_csv('/home/developer/leaderboards/newsmag_leaderboard.csv', encoding='utf-8', index=False)

print 'Script completed'

dfhtml = df.to_html().replace('<table border="1" class="dataframe">','<table class="table table-striped">') # use bootstrap styling

html_string = '''
<html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
    </head>
    <body>
       <h1>NPR One Leaderboard</h1>
''' + dfhtml + '''
    </body>
</html>'''

f = open('/var/www/news_mag_leaderboard.html','w')
f.write(html_string)
f.close()

print 'done saving HTML table'



