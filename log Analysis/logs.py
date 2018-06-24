#!/usr/bin/env python

'''using psycopg2 adapter'''
import psycopg2


from datetime import date


def connec():
	try:
		conn = psycopg2.connect("dbname = news")
		return conn.cursor()
	except:
		print"unable to connect"
		sys.exit()

query_1 = "select articles.title ,count(log.status) as view \
         from articles,log where log.path=concat('/article/',articles.slug) \
         and log.status='200 OK'group by articles.title order by view desc  limit 3"

query_2 = "select authors.name,count(log.status) as \
           views from authors,articles ,\
            log where authors.id=articles.author and \
			log.path=concat('/article/',articles.slug) \
			and status='200 OK' group by authors.name order by views desc "

query_3 = "select total.date ,cast(100.0*error.view*1.0/total.view*1 \
			as decimal(10,2) ) as views from total ,error \
			where total.date=error.date and \
			(error.view/total.view*100) > 1 order by views "


def execute_query(query):
	
	'''connect to database news'''
	cur = connec()
	'''establish the cursor to query psql'''
	cur.execute(query) 
	rows = cur.fetchall()
	cur.close()
	if (query == query_1):
		print"\nThree best titles by views\n"
		for row in rows:
			'''using index to print result'''
			print " ", row[0], "   ", row[1]
	elif(query == query_2):
		print "\n Best author by views\n"
		for row in rows:
			print " ", row[0], "  ", row[1]		
	else:
		print "\n Error response more than 1%\n"
		for row in rows:
			print row[0], "  ", row[1], "percent", "views"

if __name__ == '__main__':
	execute_query(query_1)
	execute_query(query_2)
	execute_query(query_3)
