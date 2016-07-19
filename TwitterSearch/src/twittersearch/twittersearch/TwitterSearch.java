package twittersearch.twittersearch;

import java.io.BufferedWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

import twitter4j.*;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSearch
{
	private static final String CONSUMER_KEY		= "kK6grJOBfqpsHbMaVNbnrgFVL";
	private static final String CONSUMER_SECRET = "FvOq4JjtEPpWNRDWgOJrtNlXIXHYbSqK4KPZVowy2Qos5PSuP2";
	private static int TWEETS_PER_QUERY		= 100;
	private static int MAX_QUERIES			= 600;	
	
	public static OAuth2Token getOAuth2Token()
	{
		OAuth2Token token = null;
		ConfigurationBuilder cb;
		
		cb = new ConfigurationBuilder();
		cb.setApplicationOnlyAuthEnabled(true);

		cb.setOAuthConsumerKey(CONSUMER_KEY).setOAuthConsumerSecret(CONSUMER_SECRET);

		try
		{
			token = new TwitterFactory(cb.build()).getInstance().getOAuth2Token();
		}
		catch (Exception e)
		{
			System.out.println("Could not get OAuth2 token");
			e.printStackTrace();
			System.exit(0);
		}
		return token;
	}
	
	public static Twitter getTwitter()
	{
		OAuth2Token token;

		//get token
		token = getOAuth2Token();
		
		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setApplicationOnlyAuthEnabled(true);

		cb.setOAuthConsumerKey(CONSUMER_KEY);
		cb.setOAuthConsumerSecret(CONSUMER_SECRET);

		cb.setOAuth2TokenType(token.getTokenType());
		cb.setOAuth2AccessToken(token.getAccessToken());

		//	And create the Twitter object!
		return new TwitterFactory(cb.build()).getInstance();
	}
	
	public static void getData(Twitter twitter, Configuration conf, String dest, String fileName, String startInt, String endInt, String queryItem) throws IOException, URISyntaxException
	{		
		long maxID = -1;		
		
		FileSystem hdfs = FileSystem.get( new URI(dest), conf );		
		
		{
			Path file = new Path(dest + "/" + fileName);
		
			OutputStream os = hdfs.create( file, new Progressable() {
		        public void progress() {
		        	System.out.print(".");
		        }});
		
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		
			try
			{			
				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");

				//	This finds the rate limit specifically for doing the search API call we use in this program
				RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");	

				//	This is the loop that retrieve multiple blocks of tweets from Twitter
				for (int queryNumber=0;queryNumber < MAX_QUERIES; queryNumber++)
					//while(true)
				{				
					if(searchTweetsRateLimit.getRemaining() == 0)
					{					
						System.out.printf("!!! Sleeping for %d seconds due to rate limits\n", searchTweetsRateLimit.getSecondsUntilReset());				
						Thread.sleep((searchTweetsRateLimit.getSecondsUntilReset()+2) * 1000l);
					}

					Query q = new Query(queryItem);			// Search for tweets that contains this term
					q.setCount(TWEETS_PER_QUERY);				// How many tweets, max, to retrieve
					q.since(startInt).until(endInt);			// Get all tweets from specific time interval
					q.setLang("en");	  						// English language tweets, please
					q.setResultType(Query.MIXED);

					if (maxID != -1)
					{
						q.setMaxId(maxID - 1);
					}
					//	This actually does the search on Twitter and makes the call across the network
					QueryResult r = twitter.search(q);
				
					if (r.getTweets().size() == 0)
					{
						System.out.println("Done...");
						break;			
					}
				
					for (Status s: r.getTweets())				
					{
						if (maxID == -1 || s.getId() < maxID)
						{
							maxID = s.getId();
						}
						br.write(s.getText());						
					}				
					searchTweetsRateLimit = r.getRateLimitStatus();
				}
			}
			catch (Exception e)
			{			
				System.out.println("Exception while retreiving twitter data!!!!!!");
				e.printStackTrace();
			}
		}		
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String dest = args[0];
		String fileName = args[1];
		String startInt = args[2];
		String endInt = args[3];
		String queryItem = args[4];
		Twitter twitter = getTwitter();
		getData(twitter, conf, dest, fileName, startInt, endInt, queryItem);
		
		/*
		 * hadoop jar TwitterSearch-0.0.1-SNAPSHOT.jar twittersearch.twittersearch.TwitterSearch /user/ass150430/Twitter_Input data1.txt 2016-06-12 2016-06-13 trump
		 */
		
		/*
		 *  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/013/a/as/ass150430/twitter4j-core-4.0.3.jar
		 */
	}		
}

