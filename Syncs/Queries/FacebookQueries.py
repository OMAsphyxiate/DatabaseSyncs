import sys, datetime, facebook
from DatabaseSyncs import DBFunctions as dbf
import Connect as ct

daterange = datetime.datetime.now() - datetime.timedelta(days=60)
graph = facebook.GraphAPI(ct.FACEBOOK_USER_TOKEN)

#Post Level - Consumption Type
PostConsumptionTypeInsert = 'INSERT INTO "Facebook"."consumptiontype" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostConsumptionTypeGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_interests_consumptions_by_type).period(lifetime)'
def PostConsumptionType():
	PostConsumptionTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_interests_consumptions_by_type).period(lifetime)')

		for post in posts['data']:
			var1 = post['id']
			try:
				var2 = post['permalink_url']
			except:
				var2 = 'No URL'
			try:
				var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
			except:
				var3 = 'No Message'
			try:
				var4 = post['type']
			except:
				var4 = 'No Type'
			var5 = post['created_time']
			try:
				var6 = post['insights']['data'][0]['name']
			except:
				var6 = 'No Insights Name'
			try:
				var7 = post['insights']['data'][0]['period']
			except:
				var7 = 'No Insights Period'
			try:
				var8 = post['insights']['data'][0]['values'][0]['value']['video play'].replace('{}', 0)
				if var8 == "":
					var8=0
				else:
					pass
			except:
				var8 = 0
			try:
				var9 = post['insights']['data'][0]['values'][0]['value']['other clicks'].replace('{}', 0)
				if var9 == "":
					var9=0
				else:
					pass
			except:
				var9 = 0
			try:
				var10 = post['insights']['data'][0]['values'][0]['value']['photo view'].replace('{}', 0)
				if var10 == "":
					var10=0
				else:
					pass
			except:
				var10 = 0
			try:
				var11 = post['insights']['data'][0]['values'][0]['value']['link clicks'].replace('{}', 0)
				if var11 == "":
					var11=0
				else:
					pass
			except:
				var11 = 0
			insertString = dbf.TupleList(dbf.ListShaping(PostConsumptionTypeList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10,var11))) #Write data to file

	injectionString = PostConsumptionTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Consumption Type")
		else:
			raise
	except:
		print("FAILED: Raw Consumption Type")
		#print(injectionString)
#Post Level - Consumption Unique
PostConsumptionUniqueInsert = 'INSERT INTO "Facebook"."consumptionunique" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostConsumptionUniqueGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_consumptions_by_type_unique).period(lifetime)'
def PostConsumptionUnique():
	PostConsumptionUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_consumptions_by_type_unique).period(lifetime)')

		for post in posts['data']:
			var1 = post['id']
			try:
				var2 = post['permalink_url']
			except:
				var2 = 'No Permanent Link'
			try:
				var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
			except:
				var3 = 'No Message'
			try:
				var4 = post['type']
			except:
				var4 = 'No Type'
			var5 = post['created_time']
			try:
				var6 = post['insights']['data'][0]['name']
			except:
				var6 = 'No Insights Name'
			try:
				var7 = post['insights']['data'][0]['period']
			except:
				var7 = 'No Insights Period'
			try:
				var8 = post['insights']['data'][0]['values'][0]['value']['video play']
			except:
				var8 = 0
			try:
				var9 = post['insights']['data'][0]['values'][0]['value']['other clicks']
			except:
				var9 = 0
			try:
				var10 = post['insights']['data'][0]['values'][0]['value']['photo view']
			except:
				var10 = 0
			try:
				var11 = post['insights']['data'][0]['values'][0]['value']['link clicks']
			except:
				var11 = 0
			insertString = dbf.TupleList(dbf.ListShaping(PostConsumptionUniqueList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10,var11)))#Write data to file
	injectionString = PostConsumptionUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Consumption Unique")
		else:
			raise
	except:
		print("FAILED: Post Consumption Unique")
#Post Level - Key Metrics
PostKeyMetricsInsert = 'INSERT INTO "Facebook"."keymetricspost" VALUES {0} ON CONFLICT (insightid) DO NOTHING'
PostKeyMetricsGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_impressions_unique,post_impressions_organic_unique,post_impressions_paid_unique,post_impressions_organic,post_impressions_paid,post_engaged_users,post_consumptions_unique,post_consumptions,post_negative_feedback,post_impressions_fan,post_impressions_fan_unique,post_impressions_fan_paid,post_impressions_fan_paid_unique,post_engaged_fan,post_video_complete_views_organic_unique,post_video_complete_views_organic,post_video_complete_views_paid_unique,post_video_complete_views_paid,post_video_views_organic_unique,post_video_views_organic,post_video_views_paid_unique,post_video_views_paid,post_video_avg_time_watched,post_video_length).period(lifetime)'
def PostKeyMetrics():
	PostKeyMetricsList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_impressions_unique,post_impressions_organic_unique,post_impressions_paid_unique,post_impressions_organic,post_impressions_paid,post_engaged_users,post_consumptions_unique,post_consumptions,post_negative_feedback,post_impressions_fan,post_impressions_fan_unique,post_impressions_fan_paid,post_impressions_fan_paid_unique,post_engaged_fan,post_video_complete_views_organic_unique,post_video_complete_views_organic,post_video_complete_views_paid_unique,post_video_complete_views_paid,post_video_views_organic_unique,post_video_views_organic,post_video_views_paid_unique,post_video_views_paid,post_video_avg_time_watched,post_video_length).period(lifetime)')

		for post in posts['data']:
			try:
				var1 = post['message']
			except:
				var1 = "No Message"
			for key in post['insights']:
				for entry in post['insights']['data']:
					for value in entry['values']:
						insertString = dbf.TupleList(dbf.ListShaping(PostKeyMetricsList,(str(item),post['id'],post['permalink_url'],var1,post['type'],post['created_time'],\
						entry['name'],entry['period'],entry['title'],entry['description'],entry['id'],value['value'])))
	injectionString = PostKeyMetricsInsert.format(insertString)
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Key Metrics")
		else:
			raise
	except:
		print("FAILED: Post Key Metrics")
#Post Level - Negative Feedback Type
PostNegativeFeedbackTypeInsert = 'INSERT INTO "Facebook"."negativefeedbacktype" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostNegativeFeedbackTypeGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_negative_feedback_by_type).period(lifetime)'
def PostNegativeFeedbackType():
	PostNegativeFeedbackTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_negative_feedback_by_type).period(lifetime)')

		for post in posts['data']	:
			var1 = post['id']
			try:
				var2 = post['permalink_url']
			except:
				var2 = 'No Permanent Link'
			try:
				var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
			except:
				var3 = 'No Message'
			try:
				var4 = post['type']
			except:
				var4 = 'No Type'
			var5 = post['created_time']
			try:
				var6 = post['insights']['data'][0]['name']
			except:
				var6 = 'No Insights Name'
			try:
				var7 = post['insights']['data'][0]['period']
			except:
				var7 = 'No Insights Period'
			try:
				var8 = post['insights']['data'][0]['values'][0]['value']['hide_all_clicks']
				if var8 == "":
					var8=0
				else:
					pass
			except:
				var8 = 0
			try:
				var9 = post['insights']['data'][0]['values'][0]['value']['hide_clicks']
				if var9 == "":
					var9=0
				else:
					pass
			except:
				var9 = 0
			try:
				var10 = post['insights']['data'][0]['values'][0]['value']['report_spam_clicks']
				if var10 == "":
					var10=0
				else:
					pass
			except:
				var10 = 0
			insertString = dbf.TupleList(dbf.ListShaping(PostNegativeFeedbackTypeList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10))) #Write data to file
	injectionString = PostNegativeFeedbackTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Negative Feedback Type")
		else:
			raise
	except:
		print("FAILED: Post Negative Feedback Type")

#Post Level - Negative Feedback Type Unique
PostNegativeFeedbackTypeUniqueInsert = 'INSERT INTO "Facebook"."negativefeedbacktypeunique" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostNegativeFeedbackTypeUniqueGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_negative_feedback_by_type_unique).period(lifetime)'
def PostNegativeFeedbackTypeUnique():
	PostNegativeFeedbackTypeUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_negative_feedback_by_type_unique).period(lifetime)')	
		for post in posts['data']:
			var1 = post['id']
			try:
				var2 = post['permalink_url']
			except:
				var2 = 'No Permanent Link'
			try:
				var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
			except:
				var3 = 'No Message'
			try:
				var4 = post['type']
			except:
				var4 = 'No Type'
			var5 = post['created_time']
			try:
				var6 = post['insights']['data'][0]['name']
			except:
				var6 = 'No Insights Name'
			try:
				var7 = post['insights']['data'][0]['period']
			except:
				var7 = 'No Insights Period'
			try:
				var8 = post['insights']['data'][0]['values'][0]['value']['hide_all_clicks']
				if var8 == "":
					var8=0
				else:
					pass
			except:
				var8 = 0
			try:
				var9 = post['insights']['data'][0]['values'][0]['value']['hide_clicks']
				if var9 == "":
					var9=0
				else:
					pass
			except:
				var9 = 0
			try:
				var10 = post['insights']['data'][0]['values'][0]['value']['report_spam_clicks']
				if var10 == "":
					var10=0
				else:
					pass
			except:
				var10 = 0
			insertString = dbf.TupleList(dbf.ListShaping(PostNegativeFeedbackTypeUniqueList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10))) #Write data to file
	injectionString = PostNegativeFeedbackTypeUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Negative Feedback Type Unique")
		else:
			raise
	except:
		print("FAILED: Post Negative Feedback Type Unique")

#Post Level - Story Action Type
PostStoryActionTypeInsert = 'INSERT INTO "Facebook"."storyactiontype" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostStoryActionTypeGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_story_adds_by_action_type).period(lifetime)'
def PostStoryActionType():
	PostStoryActionTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_story_adds_by_action_type).period(lifetime)')
		for post in posts['data']:
				var1 = post['id']
				try:
					var2 = post['permalink_url']
				except:
					var2 = 'No Permanent Link'
				try:
					var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
				except:
					var3 = 'No Message'
				try:
					var4 = post['type']
				except:
					var4 = 'No Type'
				var5 = post['created_time']
				try:
					var6 = post['insights']['data'][0]['name']
				except:
					var6 = 'No Insights Name'
				try:
					var7 = post['insights']['data'][0]['period']
				except:
					var7 = 'No Insights Period'
				try:
					var8 = post['insights']['data'][0]['values'][0]['value']['like']
					if var8 == "":
						var8=0
					else:
						pass
				except:
					var8 = 0
				try:
					var9 = post['insights']['data'][0]['values'][0]['value']['comment']
					if var9 == "":
						var9=0
					else:
						pass
				except:
					var9 = 0
				try:
					var10 = post['insights']['data'][0]['values'][0]['value']['share']
					if var10 == "":
						var10=0
					else:
						pass
				except:
					var10 = 0
				insertString = dbf.TupleList(dbf.ListShaping(PostStoryActionTypeList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10))) #Write data to file
	injectionString = PostStoryActionTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Story Action Type")
		else:
			raise
	except:
		print("FAILED: Post Story Action Type")

#Post Level - Story Action Type Unique
PostStoryActionTypeUniqueInsert = 'INSERT INTO "Facebook"."storyactiontypeunique" VALUES {0} ON CONFLICT (postid) DO NOTHING'
PostStoryActionTypeUniqueGrab = 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_story_adds_by_action_type_unique).period(lifetime)'
def PostStoryActionTypeUnique():
	PostStoryActionTypeUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'posts?fields=id,permalink_url,message,type,created_time,insights.metric(post_story_adds_by_action_type_unique).period(lifetime)')
		for post in posts['data']:
			var1 = post['id']
			try:
				var2 = post['permalink_url']
			except:
				var2 = 'No Permanent Link'
			try:
				var3 = post['message'].replace("'","").replace('\n',' ').replace('"','')
			except:
				var3 = 'No Message'
			try:
				var4 = post['type']
			except:
				var4 = 'No Type'
			var5 = post['created_time']
			try:
				var6 = post['insights']['data'][0]['name']
			except:
				var6 = 'No Insights Name'
			try:
				var7 = post['insights']['data'][0]['period']
			except:
				var7 = 'No Insights Period'
			try:
				var8 = post['insights']['data'][0]['values'][0]['value']['like']
				if var8 == "":
					var8=0
				else:
					pass
			except:
				var8 = 0
			try:
				var9 = post['insights']['data'][0]['values'][0]['value']['comment']
				if var9 == "":
					var9=0
				else:
					pass
			except:
				var9 = 0
			try:
				var10 = post['insights']['data'][0]['values'][0]['value']['share']
				if var10 == "":
					var10=0
				else:
					pass
			except:
				var10 = 0
			insertString = dbf.TupleList(dbf.ListShaping(PostStoryActionTypeUniqueList,(str(item),var1,var2,var3,var4,var5,var6,var7,var8,var9,var10))) #Write data to file
	injectionString = PostStoryActionTypeUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Post Story Action Type Unique")
		else:
			raise
	except:
		print("FAILED: Post Story Action Type Unique")

#Raw Level - Consumption By Type
RawConsumptionByTypeInsert = 'INSERT INTO "Facebook"."consumptionbytype" VALUES {0} ON CONFLICT (facebookid,name,dateperiod,enddate) DO NOTHING'
RawConsumptionByTypeGrab = 'insights/page_consumptions_by_consumption_type?since=%s' %daterange
def RawConsumptionByType():
	RawConsumptionByTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], RawConsumptionByTypeGrab)
		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['video play']
				except:
					var20 = 0
				try:
					var21 = value['value']['other clicks']
				except:
					var21 = 0
				try:
					var22 = value['value']['photo view']
				except:
					var22 = 0
				try:
					var23 = value['value']['link clicks']
				except:
					var23 = 0
				try:
					var24 = value['value']['button clicks']
				except:
					var24 = 0
				var25 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawConsumptionByTypeList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25)))
	injectionString = RawConsumptionByTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Consumption By Type")
		else:
			raise
	except:
		print("FAILED: Raw Consumption By Type")

#Raw Level - External Referral
RawExternalReferralInsert = 'INSERT INTO "Facebook"."externalreferrals" VALUES {0} ON CONFLICT (facebookid, name, domain, enddate) DO NOTHING'
RawExternalReferralGrab = 'insights/page_views_external_referrals?since=%s' %daterange
def RawExternalReferral():
	RawExternalReferralList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_views_external_referrals?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				if 'value' in entry:
					for key, value in entry['value'].items():
						insertString = dbf.TupleList(dbf.ListShaping(RawExternalReferralList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
				else:
					pass
	injectionString = RawExternalReferralInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw External Referral")
		else:
			raise
	except:
		print("FAILED: Raw External Referral")

#Raw Level - Fans by Age & Gender
RawFansByAgeGenderInsert = 'INSERT INTO "Facebook"."fansagegender" VALUES {0} ON CONFLICT (facebookid, name, dateperiod, enddate) DO NOTHING'
RawFansByAgeGenderGrab = 'insights/page_fans_gender_age?since=%s' %daterange
def RawFansByAgeGender():
	RawFansByAgeGenderList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_gender_age?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['F.13-17']
				except:
					var20 = 0
				try:
					var21 = value['value']['F.18-24']
				except:
					var21 = 0
				try:
					var22 = value['value']['F.25-34']
				except:
					var22 = 0
				try:
					var23 = value['value']['F.35-44']
				except:
					var23 = 0
				try:
					var24 = value['value']['F.45-54']
				except:
					var24 = 0
				try:
					var25 = value['value']['F.55-64']
				except:
					var25 = 0
				try:
					var26 = value['value']['F.65+']
				except:
					var26 = 0
				try:
					var27 = value['value']['M.13-17']
				except:
					var27 = 0
				try:
					var28 = value['value']['M.18-24']
				except:
					var28 = 0
				try:
					var29 = value['value']['M.25-34']
				except:
					var29 = 0
				try:
					var30 = value['value']['M.35-44']
				except:
					var30 = 0
				try:
					var31 = value['value']['M.45-54']
				except:
					var31 = 0
				try:
					var32 = value['value']['M.55-64']
				except:
					var32 = 0
				try:
					var33 = value['value']['M.65+']
				except:
					var33 = 0
				try:
					var34 = value['value']['U.13-17']
				except:
					var34 = 0
				try:
					var35 = value['value']['U.18-24']
				except:
					var35 = 0
				try:
					var36 = value['value']['U.25-34']
				except:
					var36 = 0
				try:
					var37 = value['value']['U.35-44']
				except:
					var37 = 0
				try:
					var38 = value['value']['U.45-54']
				except:
					var38 = 0
				try:
					var39 = value['value']['U.55-64']
				except:
					var39 = 0
				try:
					var40 = value['value']['U.65+']
				except:
					var40 = 0
				var41 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawFansByAgeGenderList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33,var34,var35,var36,var37,var38,var39,var40,var41)))
	injectionString = RawFansByAgeGenderInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans By Age & Gender")
		else:
			raise
	except:
		print("FAILED: Raw Fans By Age & Gender")

#Raw Level - Fans by City
RawFansByCityInsert = 'INSERT INTO "Facebook"."fansbycity" VALUES {0} ON CONFLICT (facebookid, city, enddate) DO NOTHING'
RawFansByCityGrab = 'insights/page_fans_city?since=%s' %daterange
def RawFansByCity():
	RawFansByCityList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_city?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				for key, value in entry['value'].items():
					insertString = dbf.TupleList(dbf.ListShaping(RawFansByCityList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
	injectionString = RawFansByCityInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans by City")
		else:
			raise
	except:
		print("FAILED: Raw Fans by City")

#Raw Level - Fans by Country
RawFansByCountryInsert = 'INSERT INTO "Facebook"."fansbycountry" VALUES {0} ON CONFLICT (facebookid,country,enddate) DO NOTHING'
RawFansByCountryGrab = 'insights/page_fans_country?since=%s' %daterange
def RawFansByCountry():
	RawFansByCountryList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_country?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				for key, value in entry['value'].items():
					insertString = dbf.TupleList(dbf.ListShaping(RawFansByCountryList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
	injectionString = RawFansByCountryInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans by Country")
		else:
			raise
	except:
		print("FAILED: Raw Fans by Country")

#Raw Level - Fans by Like Source
RawFansByLikeSourceInsert = 'INSERT INTO "Facebook"."fansbylikesource" VALUES {0} ON CONFLICT (dataid, source, enddate) DO NOTHING'
RawFansByLikeSourceGrab = 'insights/page_fans_by_like_source_unique?since=%s' %daterange
def RawFansByLikeSource():
	RawFansByLikeSourceList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_by_like_source_unique?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				if 'value' in entry:
					for key, value in entry['value'].items():
						insertString = dbf.TupleList(dbf.ListShaping(RawFansByLikeSourceList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
				else:
					pass
	injectionString = RawFansByLikeSourceInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans by Like Source")
		else:
			raise
	except:
		print("FAILED: Raw Fans by Like Source")

#Raw Level - Fans by Locale
RawFansByLocaleInsert = 'INSERT INTO "Facebook"."fansbylocale" VALUES {0} ON CONFLICT (dataid,language,enddate) DO NOTHING'
RawFansByLocaleGrab = 'insights/page_fans_locale?since=%s' %daterange
def RawFansByLocale():
	RawFansByLocaleList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_locale?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				if 'value' in entry:
					for key, value in entry['value'].items():
						insertString = dbf.TupleList(dbf.ListShaping(RawFansByLocaleList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
				else:
					pass
	injectionString = RawFansByLocaleInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans by Locale")
		else:
			raise
	except:
		print("FAILED: Raw Fans by Locale")

#Raw Level - Fans Liked Online
RawFansLikedOnlineInsert = 'INSERT INTO "Facebook"."fanslikedonline" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawFansLikedOnlineGrab = 'insights/page_fans_online?since=%s' %daterange
def RawFansLikedOnline():
	RawFansLikedOnlineList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans_online?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['1']
				except:
					var20 = 0
				try:
					var21 = value['value']['2']
				except:
					var21 = 0
				try:
					var22 = value['value']['3']
				except:
					var22 = 0
				try:
					var23 = value['value']['4']
				except:
					var23 = 0
				try:
					var24 = value['value']['5']
				except:
					var24 = 0
				try:
					var25 = value['value']['6']
				except:
					var25 = 0
				try:
					var26 = value['value']['7']
				except:
					var26 = 0
				try:
					var27 = value['value']['8']
				except:
					var27 = 0
				try:
					var28 = value['value']['9']
				except:
					var28 = 0
				try:
					var29 = value['value']['10']
				except:
					var29 = 0
				try:
					var30 = value['value']['11']
				except:
					var30 = 0
				try:
					var31 = value['value']['12']
				except:
					var31 = 0
				try:
					var32 = value['value']['13']
				except:
					var32 = 0
				try:
					var33 = value['value']['14']
				except:
					var33 = 0
				try:
					var34 = value['value']['15']
				except:
					var34 = 0
				try:
					var35 = value['value']['16']
				except:
					var35 = 0
				try:
					var36 = value['value']['17']
				except:
					var36 = 0
				try:
					var37 = value['value']['18']
				except:
					var37 = 0
				try:
					var38 = value['value']['19']
				except:
					var38 = 0
				try:
					var39 = value['value']['20']
				except:
					var39 = 0
				try:
					var40 = value['value']['21']
				except:
					var40 = 0
				try:
					var41 = value['value']['22']
				except:
					var41 = 0
				try:
					var42 = value['value']['23']
				except:
					var42 = 0
				var43 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawFansLikedOnlineList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33,var34,var35,var36,var37,var38,var39,var40,var41,var42,var43)))
	injectionString = RawFansLikedOnlineInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Fans Liked Online")
		else:
			raise
	except:
		print("FAILED: Raw Fans Liked Online")

#Raw Level - Impression Frequency Distribution
RawImpressionFrequencyDistributionInsert = 'INSERT INTO "Facebook"."impressionfrequencydistribution" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawImpressionFrequencyDistributionGrab = 'insights/page_posts_impressions_frequency_distribution?since=%s' %daterange
def RawImpressionFrequencyDistribution():
	RawImpressionFrequencyDistributionList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_posts_impressions_frequency_distribution?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['1']
				except:
					var20 = 0
				try:
					var21 = value['value']['2']
				except:
					var21 = 0
				try:
					var22 = value['value']['3']
				except:
					var22 = 0
				try:
					var23 = value['value']['4']
				except:
					var23 = 0
				try:
					var24 = value['value']['5']
				except:
					var24 = 0
				try:
					var25 = value['value']['6-10']
				except:
					var25 = 0
				try:
					var26 = value['value']['11-20']
				except:
					var26 = 0
				try:
					var27 = value['value']['21+']
				except:
					var27 = 0
				var28 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawImpressionFrequencyDistributionList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28)))
	injectionString = RawImpressionFrequencyDistributionInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Impression Frequency Distribution")
		else:
			raise
	except:
		print("FAILED: Raw Impression Frequency Distribution")

#Raw Level - Impressions by Age & Gender
RawImpressionByAgeGenderInsert = 'INSERT INTO "Facebook"."impressionsagegender" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawImpressionByAgeGenderGrab = 'insights/page_impressions_by_age_gender_unique?since=%s' %daterange
def RawImpressionByAgeGender():
	RawImpressionByAgeGenderList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_impressions_by_age_gender_unique?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['F.13-17']
				except:
					var20 = 0
				try:
					var21 = value['value']['F.18-24']
				except:
					var21 = 0
				try:
					var22 = value['value']['F.25-34']
				except:
					var22 = 0
				try:
					var23 = value['value']['F.35-44']
				except:
					var23 = 0
				try:
					var24 = value['value']['F.45-54']
				except:
					var24 = 0
				try:
					var25 = value['value']['F.55-64']
				except:
					var25 = 0
				try:
					var26 = value['value']['F.65+']
				except:
					var26 = 0
				try:
					var27 = value['value']['M.13-17']
				except:
					var27 = 0
				try:
					var28 = value['value']['M.18-24']
				except:
					var28 = 0
				try:
					var29 = value['value']['M.25-34']
				except:
					var29 = 0
				try:
					var30 = value['value']['M.35-44']
				except:
					var30 = 0
				try:
					var31 = value['value']['M.45-54']
				except:
					var31 = 0
				try:
					var32 = value['value']['M.55-64']
				except:
					var32 = 0
				try:
					var33 = value['value']['M.65+']
				except:
					var33 = 0
				try:
					var34 = value['value']['U.13-17']
				except:
					var34 = 0
				try:
					var35 = value['value']['U.18-24']
				except:
					var35 = 0
				try:
					var36 = value['value']['U.25-34']
				except:
					var36 = 0
				try:
					var37 = value['value']['U.35-44']
				except:
					var37 = 0
				try:
					var38 = value['value']['U.45-54']
				except:
					var38 = 0
				try:
					var39 = value['value']['U.55-64']
				except:
					var39 = 0
				try:
					var40 = value['value']['U.65+']
				except:
					var40 = 0
				var41 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawImpressionByAgeGenderList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33,var34,var35,var36,var37,var38,var39,var40,var41)))
	injectionString = RawImpressionByAgeGenderInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Impressions by Age & Gender")
		else:
			raise
	except:
		print("FAILED: Raw Impressions by Age & Gender")

#Raw Level - Key Metrics
RawKeyMetricsInsert = 'INSERT INTO "Facebook"."keymetricsraw" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawKeyMetricsGrab = 'insights/page_fans,page_fan_adds_unique,page_fan_removes_unique,page_engaged_users,page_engaged_users,page_impressions_unique,page_impressions_unique,page_impressions_organic_unique,page_impressions_organic_unique,page_impressions_paid_unique,page_impressions_paid_unique,page_impressions,page_impressions,page_impressions_organic,page_impressions_organic,page_impressions_paid,page_impressions_paid,page_views_logged_in_total,page_views_logged_in_unique,page_posts_impressions_unique,page_posts_impressions_unique,page_posts_impressions_organic_unique,page_posts_impressions_organic_unique,page_posts_impressions_paid_unique,page_posts_impressions_paid_unique,page_posts_impressions,page_posts_impressions,page_posts_impressions_organic,page_posts_impressions_organic,page_posts_impressions_paid,page_posts_impressions_paid,page_consumptions_unique,page_consumptions_unique,page_consumptions,page_consumptions,page_negative_feedback_unique,page_negative_feedback_unique,page_negative_feedback,page_negative_feedback,page_places_checkin_total,page_places_checkin_total,page_places_checkin_total_unique,page_places_checkin_total_unique,page_places_checkin_mobile,page_places_checkin_mobile,page_places_checkin_mobile_unique,page_places_checkin_mobile_unique,page_video_views,page_video_views,page_video_views_autoplayed,page_video_views_autoplayed,page_video_views_paid,page_video_views_paid,page_video_views_organic,page_video_views_organic?since=%s' %daterange
def RawKeyMetrics():
	RawKeyMetricsList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_fans,page_fan_adds_unique,page_fan_removes_unique,page_engaged_users,page_engaged_users,page_impressions_unique,page_impressions_unique,page_impressions_organic_unique,page_impressions_organic_unique,page_impressions_paid_unique,page_impressions_paid_unique,page_impressions,page_impressions,page_impressions_organic,page_impressions_organic,page_impressions_paid,page_impressions_paid,page_views_logged_in_total,page_views_logged_in_unique,page_posts_impressions_unique,page_posts_impressions_unique,page_posts_impressions_organic_unique,page_posts_impressions_organic_unique,page_posts_impressions_paid_unique,page_posts_impressions_paid_unique,page_posts_impressions,page_posts_impressions,page_posts_impressions_organic,page_posts_impressions_organic,page_posts_impressions_paid,page_posts_impressions_paid,page_consumptions_unique,page_consumptions_unique,page_consumptions,page_consumptions,page_negative_feedback_unique,page_negative_feedback_unique,page_negative_feedback,page_negative_feedback,page_places_checkin_total,page_places_checkin_total,page_places_checkin_total_unique,page_places_checkin_total_unique,page_places_checkin_mobile,page_places_checkin_mobile,page_places_checkin_mobile_unique,page_places_checkin_mobile_unique,page_video_views,page_video_views,page_video_views_autoplayed,page_video_views_autoplayed,page_video_views_paid,page_video_views_paid,page_video_views_organic,page_video_views_organic?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				try:
					insertString = dbf.TupleList(dbf.ListShaping(RawKeyMetricsList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],\
					entry['value'],entry['end_time'].replace('T07:00:00+0000',''))))
				except:
					e = sys.exc_info()[0]
					print( "<p>Error: %s</p>" % e )
	injectionString = RawKeyMetricsInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Key Metrics")
		else:
			raise
	except:
		print("FAILED: Raw Key Metrics")

#Raw Level - Negative Feedback by Type
RawNegativeFeedbackByTypeInsert = 'INSERT INTO "Facebook"."negativefeedbackbytype" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawNegativeFeedbackByTypeGrab = 'insights/page_negative_feedback_by_type?since=%s' %daterange
def RawNegativeFeedbackByType():
	RawNegativeFeedbackByTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_negative_feedback_by_type?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['hide_all_clicks']
				except:
					var20 = 0
				try:
					var21 = value['value']['hide_clicks']
				except:
					var21 = 0
				try:
					var22 = value['value']['unlike_page_clicks']
				except:
					var22 = 0
				try:
					var23 = value['value']['report_spam_clicks']
				except:
					var23 = 0
				try:
					var24 = value['value']['xbutton_clicks']
				except:
					var24 = 0
				var25 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawNegativeFeedbackByTypeList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25)))
	injectionString = RawNegativeFeedbackByTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Negative Feedback Type")
		else:
			raise
	except:
		print("FAILED: Raw Negative Feedback Type")

#Raw Level - Negative Feedback by Type Unique
RawNegativeFeedbackByTypeUniqueInsert = 'INSERT INTO "Facebook"."negativefeedbackbytypeunique" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawNegativeFeedbackByTypeUniqueGrab = 'insights/page_negative_feedback_by_type_unique?since=%s' %daterange
def RawNegativeFeedbackByTypeUnique():
	RawNegativeFeedbackByTypeUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_negative_feedback_by_type_unique?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['hide_all_clicks']
				except:
					var20 = 0
				try:
					var21 = value['value']['hide_clicks']
				except:
					var21 = 0
				try:
					var22 = value['value']['unlike_page_clicks']
				except:
					var22 = 0
				try:
					var23 = value['value']['report_spam_clicks']
				except:
					var23 = 0
				try:
					var24 = value['value']['xbutton_clicks']
				except:
					var24 = 0
				var25 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawNegativeFeedbackByTypeUniqueList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25)))
	injectionString = RawNegativeFeedbackByTypeUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Negative Feedback Type Unique")
		else:
			raise
	except:
		print("FAILED: Raw Negative Feedback Type Unique")

#Raw Level - Positive Feedback by Type
RawPositiveFeedbackByTypeInsert = 'INSERT INTO "Facebook"."positivefeedbackbytype" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawPositiveFeedbackByTypeGrab = 'insights/page_positive_feedback_by_type?since=%s' %daterange
def RawPositiveFeedbackByType():
	RawPositiveFeedbackByTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_positive_feedback_by_type?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['link']
				except:
					var20 = 0
				try:
					var21 = value['value']['like']
				except:
					var21 = 0
				try:
					var22 = value['value']['comment']
				except:
					var22 = 0
				try:
					var23 = value['value']['claim']
				except:
					var23 = 0
				try:
					var24 = value['value']['answer']
				except:
					var24 = 0
				try:
					var25 = value['value']['other']
				except:
					var25 = 0
				var26 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawPositiveFeedbackByTypeList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26)))
	injectionString = RawPositiveFeedbackByTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Positive Feedback by Type")
		else:
			raise
	except:
		print("FAILED: Raw Positive Feedback by Type")

#Raw Level - Positive Feedback by Type Unique
RawPositiveFeedbackByTypeUniqueInsert = 'INSERT INTO "Facebook"."positivefeedbackbytypeunique" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawPositiveFeedbackByTypeUniqueGrab = 'insights/page_positive_feedback_by_type_unique?since=%s' %daterange
def RawPositiveFeedbackByTypeUnique():
	RawPositiveFeedbackByTypeUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_positive_feedback_by_type_unique?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['link']
				except:
					var20 = 0
				try:
					var21 = value['value']['like']
				except:
					var21 = 0
				try:
					var22 = value['value']['comment']
				except:
					var22 = 0
				try:
					var23 = value['value']['claim']
				except:
					var23 = 0
				try:
					var24 = value['value']['answer']
				except:
					var24 = 0
				try:
					var25 = value['value']['other']
				except:
					var25 = 0
				var26 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawPositiveFeedbackByTypeUniqueList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26)))
	injectionString = RawPositiveFeedbackByTypeUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Positive Feedback by Type Unique")
		else:
			raise
	except:
		print("FAILED: Raw Positive Feedback by Type Unique")

#Raw Level - Stories by Story Type
RawStoriesByStoryTypeInsert = 'INSERT INTO "Facebook"."storiesbystorytype" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawStoriesByStoryTypeGrab = 'insights/page_stories_by_story_type?since=%s' %daterange
def RawStoriesByStoryType():
	RawStoriesByStoryTypeList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_stories_by_story_type?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['user post']
				except:
					var20 = 0
				try:
					var21 = value['value']['checkin']
				except:
					var21 = 0
				try:
					var22 = value['value']['fan']
				except:
					var22 = 0
				try:
					var23 = value['value']['question']
				except:
					var23 = 0
				try:
					var24 = value['value']['coupon']
				except:
					var24 = 0
				try:
					var25 = value['value']['event']
				except:
					var25 = 0
				try:
					var26 = value['value']['mention']
				except:
					var26 = 0
				try:
					var27 = value['value']['other']
				except:
					var27 = 0
				var28 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawStoriesByStoryTypeList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28)))
	injectionString = RawStoriesByStoryTypeInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Stories by Story Type")
		else:
			raise
	except:
		print("FAILED: Raw Stories by Story Type")

#Raw Level - Story by Age & Gender Unique
RawStoryByAgeGenderUniqueInsert = 'INSERT INTO "Facebook"."storybyagegenderunique" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawStoryByAgeGenderUniqueGrab = 'insights/page_story_adds_by_age_gender_unique?since=%s' %daterange
def RawStoryByAgeGenderUnique():
	RawStoryByAgeGenderUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_story_adds_by_age_gender_unique?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['F.13-17']
				except:
					var20 = 0
				try:
					var21 = value['value']['F.18-24']
				except:
					var21 = 0
				try:
					var22 = value['value']['F.25-34']
				except:
					var22 = 0
				try:
					var23 = value['value']['F.35-44']
				except:
					var23 = 0
				try:
					var24 = value['value']['F.45-54']
				except:
					var24 = 0
				try:
					var25 = value['value']['F.55-64']
				except:
					var25 = 0
				try:
					var26 = value['value']['F.65+']
				except:
					var26 = 0
				try:
					var27 = value['value']['M.13-17']
				except:
					var27 = 0
				try:
					var28 = value['value']['M.18-24']
				except:
					var28 = 0
				try:
					var29 = value['value']['M.25-34']
				except:
					var29 = 0
				try:
					var30 = value['value']['M.35-44']
				except:
					var30 = 0
				try:
					var31 = value['value']['M.45-54']
				except:
					var31 = 0
				try:
					var32 = value['value']['M.55-64']
				except:
					var32 = 0
				try:
					var33 = value['value']['M.65+']
				except:
					var33 = 0
				try:
					var34 = value['value']['U.13-17']
				except:
					var34 = 0
				try:
					var35 = value['value']['U.18-24']
				except:
					var35 = 0
				try:
					var36 = value['value']['U.25-34']
				except:
					var36 = 0
				try:
					var37 = value['value']['U.35-44']
				except:
					var37 = 0
				try:
					var38 = value['value']['U.45-54']
				except:
					var38 = 0
				try:
					var39 = value['value']['U.55-64']
				except:
					var39 = 0
				try:
					var40 = value['value']['U.65+']
				except:
					var40 = 0
				var41 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawStoryByAgeGenderUniqueList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33,var34,var35,var36,var37,var38,var39,var40,var41)))
	injectionString = RawStoryByAgeGenderUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Stories by Story Type")
		else:
			raise
	except:
		print("FAILED: Raw Stories by Story Type")

#Raw Level - Story by City Unique
RawStoryByCityUniqueInsert = 'INSERT INTO "Facebook"."storybycityunique" VALUES {0} ON CONFLICT (dataid,enddate,city) DO NOTHING'
RawStoryByCityUniqueGrab = 'insights/page_story_adds_by_city_unique?since=%s' %daterange
def RawStoryByCityUnique():
	RawStoryByCityUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_story_adds_by_city_unique?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				for key, value in entry['value'].items():
					insertString = dbf.TupleList(dbf.ListShaping(RawStoryByCityUniqueList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
	injectionString = RawStoryByCityUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Story by City Unique")
		else:
			raise
	except:
		print("FAILED: Raw Story by City Unique")

#Raw Level - Story by Country Unique
RawStoryByCountryUniqueInsert = 'INSERT INTO "Facebook"."storybycountryunique" VALUES {0} ON CONFLICT (dataid,enddate,country) DO NOTHING'
RawStoryByCountryUniqueGrab = 'insights/page_story_adds_by_country_unique?since=%s' %daterange
def RawStoryByCountryUnique():
	RawStoryByCountryUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_story_adds_by_country_unique?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				for key, value in entry['value'].items():
					insertString = dbf.TupleList(dbf.ListShaping(RawStoryByCountryUniqueList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
	injectionString = RawStoryByCountryUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Story by Country Unique")
		else:
			raise
	except:
		print("FAILED: Raw Story by Country Unique")

#Raw Level - Storyteller by Age & Gender
RawStorytellerByAgeGenderInsert = 'INSERT INTO "Facebook"."storytelleragegender" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawStorytellerByAgeGenderGrab = 'insights/page_storytellers_by_age_gender?since=%s' %daterange
def RawStorytellerByAgeGender():
	RawStorytellerByAgeGenderList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_storytellers_by_age_gender?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['F.13-17']
				except:
					var20 = 0
				try:
					var21 = value['value']['F.18-24']
				except:
					var21 = 0
				try:
					var22 = value['value']['F.25-34']
				except:
					var22 = 0
				try:
					var23 = value['value']['F.35-44']
				except:
					var23 = 0
				try:
					var24 = value['value']['F.45-54']
				except:
					var24 = 0
				try:
					var25 = value['value']['F.55-64']
				except:
					var25 = 0
				try:
					var26 = value['value']['F.65+']
				except:
					var26 = 0
				try:
					var27 = value['value']['M.13-17']
				except:
					var27 = 0
				try:
					var28 = value['value']['M.18-24']
				except:
					var28 = 0
				try:
					var29 = value['value']['M.25-34']
				except:
					var29 = 0
				try:
					var30 = value['value']['M.35-44']
				except:
					var30 = 0
				try:
					var31 = value['value']['M.45-54']
				except:
					var31 = 0
				try:
					var32 = value['value']['M.55-64']
				except:
					var32 = 0
				try:
					var33 = value['value']['M.65+']
				except:
					var33 = 0
				try:
					var34 = value['value']['U.13-17']
				except:
					var34 = 0
				try:
					var35 = value['value']['U.18-24']
				except:
					var35 = 0
				try:
					var36 = value['value']['U.25-34']
				except:
					var36 = 0
				try:
					var37 = value['value']['U.35-44']
				except:
					var37 = 0
				try:
					var38 = value['value']['U.45-54']
				except:
					var38 = 0
				try:
					var39 = value['value']['U.55-64']
				except:
					var39 = 0
				try:
					var40 = value['value']['U.65+']
				except:
					var40 = 0
				var41 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawStorytellerByAgeGenderList,(str(item), var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33,var34,var35,var36,var37,var38,var39,var40,var41)))
	injectionString = RawStorytellerByAgeGenderInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Storyteller by Age & Gender")
		else:
			raise
	except:
		print("FAILED: Raw Storyteller by Age & Gender")

#Raw Level - Storyteller by Country
RawStorytellerByCountryInsert = 'INSERT INTO "Facebook"."storytellerbycountry" VALUES {0} ON CONFLICT (dataid,enddate,country) DO NOTHING'
RawStorytellerByCountryGrab = 'insights/page_storytellers_by_country?since=%s' %daterange
def RawStorytellerByCountry():
	RawStorytellerByCountryList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_storytellers_by_country?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				for key, value in entry['value'].items():
					insertString = dbf.TupleList(dbf.ListShaping(RawStorytellerByCountryList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
	injectionString = RawStorytellerByCountryInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Storyteller by Country")
		else:
			raise
	except:
		print("FAILED: Raw Storyteller by Country")

#Raw Level - Storyteller by Locale
RawStorytellerByLocaleInsert = 'INSERT INTO "Facebook"."storytellerbylocale" VALUES {0} ON CONFLICT (dataid,enddate,language) DO NOTHING'
RawStorytellerByLocaleGrab = 'insights/page_storytellers_by_locale?since=%s' %daterange
def RawStorytellerByLocale():
	RawStorytellerByLocaleList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_storytellers_by_locale?since=%s' %daterange)

		for post in posts['data']:
			for entry in post['values']:
				if 'value' in entry:
					for key, value in entry['value'].items():
						insertString = dbf.TupleList(dbf.ListShaping(RawStorytellerByLocaleList,(str(item),post['name'],post['period'],post['title'],post['description'],post['id'],key,value,entry['end_time'].replace('T07:00:00+0000',''))))
				else:
					pass
	injectionString = RawStorytellerByLocaleInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Storyteller by Locale")
		else:
			raise
	except:
		print("FAILED: Raw Storyteller by Locale")

#Raw Level - Tab View Login Unique
RawTabViewLoginUniqueInsert = 'INSERT INTO "Facebook"."tabviewloginunique" VALUES {0} ON CONFLICT (dataid,enddate) DO NOTHING'
RawTabViewLoginUniqueGrab = 'insights/page_tab_views_login_top_unique?since=%s' %daterange
def RawTabViewLoginUnique():
	RawTabViewLoginUniqueList = []
	for item in dbf.FacebookList:
		profile = graph.get_object(str(item))
		posts = graph.get_connections(profile['id'], 'insights/page_tab_views_login_top_unique?since=%s' %daterange)

		for post in posts['data']:
			var1 = post['name']
			var2 = post['period']
			var3 = post['title']
			var4 = post['description']
			var5 = post['id']
			for value in post['values']:
				try:
					var20 = value['value']['about']
				except:
					var20 = 0
				try:
					var21 = value['value']['community']
				except:
					var21 = 0
				try:
					var22 = value['value']['custom']
				except:
					var22 = 0
				try:
					var23 = value['value']['home']
				except:
					var23 = 0
				try:
					var24 = value['value']['photos']
				except:
					var24 = 0
				try:
					var25 = value['value']['posts']
				except:
					var25 = 0
				try:
					var26 = value['value']['profile_home']
				except:
					var26 = 0
				try:
					var27 = value['value']['profile_reviews']
				except:
					var27 = 0
				try:
					var28 = value['value']['reviews']
				except:
					var28 = 0
				try:
					var29 = value['value']['services']
				except:
					var29 = 0
				try:
					var30 = value['value']['videos']
				except:
					var30 = 0
				try:
					var31 = value['value']['notes']
				except:
					var31 = 0
				try:
					var32 = value['value']['album']
				except:
					var32 = 0
				var33 = value['end_time'].replace('T07:00:00+0000','')
				insertString = dbf.TupleList(dbf.ListShaping(RawTabViewLoginUniqueList,(str(item),var1,var2,var3,var4,var5,var20,var21,var22,var23,var24,var25,var26,var27,var28,var29,var30,var31,var32,var33)))
	injectionString = RawTabViewLoginUniqueInsert.format(insertString.replace('"','\''))
	try:
		ins = dbf.PGInsert(injectionString)
		if ins == True:
			print("SUCESS: Raw Tab View Login Unique")
		else:
			raise
	except:
		print("FAILED: Raw Tab View Login Unique")