Data Set Information:

This is a large data set of news items and their respective social feedback on multiple platforms: Facebook, Google+ and LinkedIn.
The collected data relates to a period of 8 months, between November 2015 and July 2016, accounting for about 100,000 news items on four different topics: economy, microsoft, obama and palestine.

Attribute Information:

#######################
# VARIABLES OF NEWS DATA #
#######################

IDLink (numeric): Unique identifier of news items
Title (string): Title of the news item according to the official media sources
Headline (string): Headline of the news item according to the official media sources
Source (string): Original news outlet that published the news item
Topic (string): Query topic used to obtain the items in the official media sources
PublishDate (timestamp): Date and time of the news items' publication
SentimentTitle (numeric): Sentiment score of the text in the news items' title
SentimentHeadline (numeric): Sentiment score of the text in the news items' headline
Facebook (numeric): Final value of the news items' popularity according to the social media source Facebook
GooglePlus (numeric): Final value of the news items' popularity according to the social media source Google+
LinkedIn (numeric): Final value of the news items' popularity according to the social media source LinkedIn

#################################
# VARIABLES OF SOCIAL FEEDBACK DATA #
#################################

IDLink (numeric): Unique identifier of news items
TS1 (numeric): Level of popularity in time slice 1 (0-20 minutes upon publication)
TS2 (numeric): Level of popularity in time slice 2 (20-40 minutes upon publication)
TS... (numeric): Level of popularity in time slice ...
TS144 (numeric): Final level of popularity after 2 days upon publication