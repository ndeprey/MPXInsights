import MySQLdb
import pandas as pd
import requests

pd.set_option('display.max_colwidth',100)

con = MySQLdb.connect('stage4-mysql.npr.org',
		      'admin',
		      'admin',
		      'infinite')

sql1 = ('SELECT ratings_story_id, count(ratings_rating) as ThumbupsAndShares '
	'FROM infinite.user_ratings '
	'WHERE ratings_rating in %(ratings)s '
	'AND ratings_origin in %(origins)s '
	'and ratings_timestamp >= Now() - interval "24" HOUR '
	'GROUP BY ratings_story_id '
	'ORDER By ThumbupsAndShares desc '
	'limit 1000')

params1 = {"ratings": ['THUMBUP','SHARE'],
	"origins": ['LEAD','CORE','BREAK','SELECTS','INVEST','EDTR','MUSICINT','OPENDOOR'] }

df = pd.read_sql(sql1, con=con, params=params1)

sql2 = ('SELECT ratings_story_id, count(ratings_rating) as totalPlays '
        'FROM infinite.user_ratings '
        'WHERE ratings_rating in %(ratings)s '
        'AND ratings_origin in %(origins)s '
        'and ratings_timestamp >= Now() - interval "24" HOUR '
        'GROUP BY ratings_story_id '
        'ORDER By totalPlays desc '
        'limit 1000')

params2 = {"ratings": ['THUMBUP','SHARE','COMPLETED','SKIP','START'],
        "origins": ['LEAD','CORE','BREAK','SELECTS','INVEST','EDTR','MUSICINT','OPENDOOR'] }

df2 = pd.read_sql(sql2, con=con, params=params2)

df = pd.merge(df, df2,
	    left_on = ['ratings_story_id'],
	    right_on = ['ratings_story_id'],
	    how = 'inner')

df['Thumbup_Share_pct'] = df['ThumbupsAndShares'] / df['totalPlays']

df['Thumbup_Share_pct'] = [100*round(i,4) for i in df['Thumbup_Share_pct']]

df = df[df['totalPlays'] >= 200]

df = df.sort(['Thumbup_Share_pct'], ascending=False)

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

df = df[['ratings_story_id','title','Thumbup_Share_pct']]

dfhtml = df.to_html(index=False).replace('<table border="1" class="dataframe">','<table class="table table-striped">') # use bootstrap styling

html_string = '''
<html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <style>body{ margin:0 100; background:whitesmoke; }</style>
    </head>
    <body>
       <h1>NPR One Leaderboard</h1>
	  <p>A list of the 10 most frequently Liked and Shared pieces among New Magazine content in the last 24 hours</p>
''' + dfhtml + '''
    </body>
</html>'''

f = open('/var/www/news_mag_leaderboard.html','w')
f.write(html_string)
f.close()

print 'done saving HTML table'



