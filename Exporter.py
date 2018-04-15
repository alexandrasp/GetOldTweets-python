# -*- coding: utf-8 -*-
import sys,getopt,datetime,codecs
if sys.version_info[0] < 3:
    import got
else:
    import got3 as got

# import python driver to use mongoDB
from pymongo import MongoClient 

import datetime

def main(argv):

	if len(argv) == 0:
		print('You must pass some parameters. Use \"-h\" to help.')
		return

	if len(argv) == 1 and argv[0] == '-h':
		f = open('exporter_help_text.txt', 'r')
		print f.read()
		f.close()
		return

	try:
		opts, args = getopt.getopt(argv, "", ("username=", "near=", "within=", "since=", "until=", "querysearch=", "toptweets", "maxtweets=", "output="))

		tweetCriteria = got.manager.TweetCriteria()
		outputFileName = "output_got.csv"

		for opt,arg in opts:
			if opt == '--username':
				tweetCriteria.username = arg

			elif opt == '--since':
				tweetCriteria.since = arg

			elif opt == '--until':
				tweetCriteria.until = arg

			elif opt == '--querysearch':
				tweetCriteria.querySearch = arg

			elif opt == '--toptweets':
				tweetCriteria.topTweets = True

			elif opt == '--maxtweets':
				tweetCriteria.maxTweets = int(arg)
			
			elif opt == '--near':
				tweetCriteria.near = '"' + arg + '"'
			
			elif opt == '--within':
				tweetCriteria.within = '"' + arg + '"'

			elif opt == '--within':
				tweetCriteria.within = '"' + arg + '"'

			elif opt == '--output':
				outputFileName = arg
		
		client = MongoClient()
		db = client.data
		collection  = db.events
		outputFile = codecs.open(outputFileName, "w+", "utf-8")

		outputFile.write('username;date;text;geo;hashtags;istraffic;what;where1;where2;where3;rotuled')

		print('Searching...\n')

		def receiveBuffer(tweets):
			for t in tweets:
				tweet = {
					'username': t.username,
					'date': t.date.strftime("%Y-%m-%d %H:%M"),
					'text': t.text,
					'geo': t.geo,
					'hashtags': t.hashtags,
					'istraffic': 'empty',
					'what': 'empty',
					'where1': 'empty',
					'where2': 'empty',
					'where3': 'empty',
					'rotuled': False
				}
				collection.insert_one(tweet).inserted_id
				outputFile.write(('\n%s;%s;"%s";%s;%s;%s;%s;%s;%s;%s;%s' % (t.username, t.date.strftime("%Y-%m-%d %H:%M"), t.text, t.geo, t.hashtags, 'empty', 'empty', 'empty', 'empty', 'empty', 'empty')))
			outputFile.flush()
			print('More %d saved on file...\n' % len(tweets))

		got.manager.TweetManager.getTweets(tweetCriteria, receiveBuffer)

	except arg:
		print('Arguments parser error, try -h' + arg)
	finally:
		outputFile.close()
		print('Done. Output file generated "%s".' % outputFileName)

if __name__ == '__main__':
	main(sys.argv[1:])
