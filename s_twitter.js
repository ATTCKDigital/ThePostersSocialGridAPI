var Twit = require('twit');
var Q    = require('q');

var s_mongo_util = require('./utils');

var T_KEYS =
    [
      new Twit
      ({
            consumer_key:         'lAtb5CrTy2m4cDA4MXxJUSiIW'
          , consumer_secret:      'Khgh1YYdSJ5rDsh9CFAReUhgpd4rgB09iGYZHTG6B0bw0k85LX'
          , access_token:         '184491874-E6BanL97qqg41k8A4OOhlgYE8eSGglSCDw9V4mF3'
          , access_token_secret:  'VuJnZdJHqnVxhqrO6PtE14BK1G1rlYqmgLouefjLqDTW6' 
      }),
      new Twit
      ({
            consumer_key:         'mPbPyR1yrZMr0ljLMxEutv9Du'
          , consumer_secret:      'Hl4HNaRNrdLeTM6TjBOS5LL4xRkSAaOx2T9GMuPBqdANeZUJmq'
          , access_token:         '2813978174-1K8pihp7Ifaj8HRGofjIv5cTa3Jva5qQzu9pmoI'
          , access_token_secret:  '1FblIKKmnFmJXdCXLbHQjJbvKpyls8E1iacw4MMogC9O7' 
      })
    ];
var T_COUNT=0;
exports.add_stream_filter=function(  hashtag)
{
  if(hashtag)
  {
 
        var reformatted_hashtag_ = '';
        if(hashtag && hashtag.hash)
        {
              reformatted_hashtag_= hashtag.hash;
        }
        else
        {
              reformatted_hashtag_= hashtag;
        }
    console.log("stream::Stream twitter "+hashtag);
       var stream_= (exports.get_twitter_obj()).stream('statuses/filter', { track:hashtag }, function(stream)
        {
          console.log("stream:: Stream activated.");
        });
    console.log("stream::created");
      if(stream_)
        {
          console.log("stream::Stream has been created successfully.");
                  stream_.on('tweet',function(t){
                    console.log("stream::Tweet fed on stream  hashtag"+JSON.stringify(hashtag));
                    console.log("stream::Tweet fed on stream "+JSON.stringify(t));
                    var tweets_ = []; 
                    var hashtags_=[];
                    tweets_.push(t);
                    hashtags_.push(hashtag);
                    console.log("Getting db ids for "+JSON.stringify(tweets_));
                    var mongo_aa = s_mongo_util.get_db_ids(tweets_);
                    console.log("stream::Inserting "+JSON.stringify(t));
                    s_mongo_util.insert_twitter(tweets_ , hashtag , mongo_aa); 
                  });
        }
        else{
          console.log("stream::Stream unsucessful.");
        }

        console.log("Steam for reformatted_hashtag_=="+reformatted_hashtag_);
  } 
}
exports.t_count=function()
{
    return ( ( T_COUNT++ ) % T_KEYS.length );
}
exports.get_twitter_obj=function()
{
  return T_KEYS[exports.t_count()];
}

exports.search=function( cnt , hashtag  )
{
   var deferred = Q.defer();
    console.log("exports.search("+hashtag+")");
    (exports.get_twitter_obj()).get('search/tweets', { q:  hashtag , count: cnt }, function(err, data, response) {
        if(err){
          console.log("Error on twitter search...");
          console.log(JSON.stringify(err));
          deferred.reject(err);
        }
        else{ 
        deferred.resolve(data);
        }
       
    });
    return  deferred.promise;
}
exports.transform_hashtags=function(tweet,json_obj)
{
    var deferred = Q.defer(); 
    if(json_obj && tweet)
    {
        if(tweet.entities.hashtags)
           {
             tweet.entities.hashtags.forEach(function(hashtag){ 
               var reformatted_hash = s_mongo_util.reformatHash(hashtag.text); 
                json_obj.hashtags.push(  reformatted_hash  ); 
             }); 
           }
           else
           {
             console.log("No Twitter Hashtags are found.");
           }
    }
    else
    {
      deferred.resolve(json_obj);
    }
  return deferred.promise;
}

exports.find_all_tweets = function(db_handle){
  console.log("find_all_tweets::");
  var deferred = Q.defer();
  s_mongo_util.find_all_hashes(db_handle).then(function(hashtags){
      if(hashtags && hashtags.length>0){
          var r_hashtags=[];
          hashtags.forEach(function(t_){
              r_hashtags.push(s_mongo_util.reformatHash(t_.hash));
            
          });
          
          exports.find_selected_tweets(r_hashtags,db_handle).then(function(tweets){
              deferred.resolve(tweets);
          });  
      }
    else{
      deferred.resolve([]);
    }
      
  });
  return deferred.promise;
}
exports.find_selected_tweets = function(tweet_value,db_handle){
  console.log("find_selected_tweets::"+JSON.stringify(tweet_value));
  if(tweet_value && tweet_value.length==1 && tweet_value[0]=='all')
  {
    return exports.find_all_tweets(db_handle);
  }
  else
  {
    
  var deferred = Q.defer();
  var tweet_value_regexp=[]
  if(tweet_value&&tweet_value.length>0)
    {
      var all_items=[];
        tweet_value.forEach(function(item){
          var twitter_key='twitter.'+item;
          var collection = db_handle.get(twitter_key);
          console.log("Searching twitter "+twitter_key );
          collection.find({},function(err,items){
              items.forEach(function(item_){
                all_items.push(item_);
              })
          }).then(function(){
              
               deferred.resolve(all_items);
          });
         
       });
    }
  
  return deferred.promise;
  }

}






exports.transform_tweets_np=function(hashtags,tweet)
{
  console.log("transform_tweets -->"+JSON.stringify(tweet));
  var json_obj = {};
  var media_array=[];
  var used_hashtags =[];
  
  if(tweet && (!tweet.dummy))
  {
    if(tweet.message)
      {
        //hack for new storage format.
        tweet=tweet.message; 
      }
      json_obj.gen_url="https://twitter.com/"+tweet.user.screen_name+"/status/"+tweet.id_str ;
      json_obj.created_at=tweet.created_at;
      json_obj.screen_name=tweet.user.screen_name;
      json_obj.name=tweet.user.name;
      json_obj.user_id=tweet.user.id;
      json_obj.user_id_str = tweet.user.id_str;
      json_obj.media=media_array;  
      json_obj.text_str=tweet.text;   
      json_obj.id=tweet.id;
      json_obj.id_str=tweet.id_str;
      json_obj.hashtags = used_hashtags;
      json_obj.type='twitter.search';
      json_obj.media=media_array;
      if(tweet.entities.hashtags)
       {
             tweet.entities.hashtags.forEach(function(hashtag){
               json_obj.hashtags.push(s_mongo_util.reformatHash(hashtag.text));
             }); 
       }    
       if( tweet.entities.media )
       {
        
               tweet.entities.media.forEach(function(media_item){
                  var media_item_={};
                  media_item_.social_url=media_item.url;
                  media_item_.image_url=media_item.media_url;
                  media_item_.image_url_https=media_item.media_url_https; 
             
                if(media_item.sizes)
                {
                       if(media_item.sizes.small)
                         {
                           media_item_.small_image_url=media_item.media_url+":small";
                           media_item_.small_image_url_https=media_item.media_url_https+":small";
                           media_item_.small_image_width=media_item.sizes.small.w;
                           media_item_.small_image_height=media_item.sizes.small.h;
                         }
                        if(media_item.sizes.medium)
                         {
                           media_item_.medium_image_url=media_item.media_url+":medium";
                           media_item_.medium_image_url_https=media_item.media_url_https+":medium";
                           media_item_.medium_image_width=media_item.sizes.medium.w;
                           media_item_.medium_image_height=media_item.sizes.medium.h;
                         }
                        if(media_item.sizes.medium)
                         {
                           media_item_.large_image_url=media_item.media_url+":large";
                           media_item_.large_image_url_https=media_item.media_url_https+":large";
                           media_item_.large_image_width=media_item.sizes.large.w;
                           media_item_.large_image_height=media_item.sizes.large.h;
                         }
                        if(media_item.sizes.thumb)
                         {
                           media_item_.thumb_image_url=media_item.media_url+":thumb";
                           media_item_.thumb_image_url_https=media_item.media_url_https+":thumb";
                           media_item_.thumb_image_width=media_item.sizes.thumb.w;
                           media_item_.thumb_image_height=media_item.sizes.thumb.h;
                         }                  
                } 
                 json_obj.media.push(media_item_);
          }); 
          
        
       }
      
       
  }
  return json_obj;
   
}
exports.transform_tweets=function(hashtags,tweet)
{
  console.log("transform_tweets -->"+JSON.stringify(tweet));
  var json_obj = {};
  var media_array=[];
  var used_hashtags =[];
  
  var deferred = Q.defer(); 
  var queue_of_tasks = [];

  if(tweet && (!tweet.dummy))
  {
    if(tweet.message)
      {
        //hack for new storage format.
        tweet=tweet.message; 
      }
      json_obj.gen_url="https://twitter.com/"+tweet.user.screen_name+"/status/"+tweet.id_str ;
      json_obj.created_at=tweet.created_at;
      json_obj.screen_name=tweet.user.screen_name;
      json_obj.name=tweet.user.name;
      json_obj.user_id=tweet.user.id;
      json_obj.user_id_str = tweet.user.id_str;
      json_obj.media=media_array;  
      json_obj.text_str=tweet.text;   
      json_obj.id=tweet.id;
      json_obj.id_str=tweet.id_str;
      json_obj.hashtags = used_hashtags;
      json_obj.type='twitter.search';
      json_obj.media=media_array;
      if(tweet.entities.hashtags)
       {
             tweet.entities.hashtags.forEach(function(hashtag){
               json_obj.hashtags.push(s_mongo_util.reformatHash(hashtag.text));
             }); 
       }    
       if( tweet.entities.media )
       {
        
               tweet.entities.media.forEach(function(media_item){
                  var media_item_={};
                  media_item_.social_url=media_item.url;
                  media_item_.image_url=media_item.media_url;
                  media_item_.image_url_https=media_item.media_url_https; 
             
                if(media_item.sizes)
                {
                       if(media_item.sizes.small)
                         {
                           media_item_.small_image_url=media_item.media_url+":small";
                           media_item_.small_image_url_https=media_item.media_url_https+":small";
                           media_item_.small_image_width=media_item.sizes.small.w;
                           media_item_.small_image_height=media_item.sizes.small.h;
                         }
                        if(media_item.sizes.medium)
                         {
                           media_item_.medium_image_url=media_item.media_url+":medium";
                           media_item_.medium_image_url_https=media_item.media_url_https+":medium";
                           media_item_.medium_image_width=media_item.sizes.medium.w;
                           media_item_.medium_image_height=media_item.sizes.medium.h;
                         }
                        if(media_item.sizes.medium)
                         {
                           media_item_.large_image_url=media_item.media_url+":large";
                           media_item_.large_image_url_https=media_item.media_url_https+":large";
                           media_item_.large_image_width=media_item.sizes.large.w;
                           media_item_.large_image_height=media_item.sizes.large.h;
                         }
                        if(media_item.sizes.thumb)
                         {
                           media_item_.thumb_image_url=media_item.media_url+":thumb";
                           media_item_.thumb_image_url_https=media_item.media_url_https+":thumb";
                           media_item_.thumb_image_width=media_item.sizes.thumb.w;
                           media_item_.thumb_image_height=media_item.sizes.thumb.h;
                         }                  
                } 
                 json_obj.media.push(media_item_);
          }); 
          
        
       }
      console.log("returning tweet "+JSON.stringify(json_obj));
      deferred.resolve(json_obj);
  }
  else
  {
    console.log("transform_tweets - > No tweet found.") ;  
    deferred.resolve({});
  }
  return deferred.promise;
}

exports.search_db=function(cnt,hashtags,db_handle)
{
   console.log("search_db");
  var deferred = Q.defer();//promise to return all data.
  if(hashtags)
  {
       
      
      var ht_r =[] ;
        hashtags.forEach(function(tag){
             ht_r.push(tag.substr(1));    
        }); 
        exports.find_selected_tweets(ht_r,db_handle).then(function(mongo_data){ 
                        console.log("Mongo Data returned.");
                  
                            deferred.resolve(mongo_data); 
         });
  }                                                      
  return deferred.promise;
}
exports.search_multiple=function(cnt, hashtags,db_handle,client)
{
  console.log("search_multiple");
  var deferred = Q.defer();//promise to return all data.
  if(hashtags)
  {
      var collection=db_handle.get("tweet");
      var queue_of_tasks = [],twitter_ids=[],mongo_ids_to_add=[],queue_of_parse_tasks = [], ht_r =[];
      var tweets_=[];
      var mongo_aa=[];
      hashtags.forEach(function(hashtag)
      {
        queue_of_tasks.push(  exports.search( cnt,hashtag ));  
      });
     queue_of_tasks.push(  exports.search_db( cnt, hashtags ,db_handle )); 
      Q.all(queue_of_tasks).then(function(tweet_results ){ 
          var tweets_=[];
        
          if(tweet_results){
            tweet_results.forEach(function(t_){
              if(t_){ 
                if(t_.statuses){
                   t_.statuses.forEach(function(tweet_){ 
                      tweets_.push(tweet_);
                  });
                }
                 else{
                   t_.forEach(function(tweet_){ 
                      tweets_.push(tweet_);
                  });
                 }
              }
              
            });
          }
          console.log("twitter search   - completed "+tweets_.length);
               s_mongo_util.get_db_ids(tweets_).then(function(mongo_aa){
                   console.log("mongo db associative array completed . "+tweets_.length+" Mongo AA is "+JSON.stringify(mongo_aa));
                      
                            s_mongo_util.insert_twitter(tweets_ , hashtags , mongo_aa).then(function(statuses){
                                                  console.log("insert to twitter tweets - running ");
                                                 statuses.forEach(function(status){
                                                   console.log("twitter status pushed " ); 
                                                    queue_of_parse_tasks.push(exports.transform_tweets_np(hashtags,status));
                                                 }) ; 
                                                deferred.resolve(queue_of_parse_tasks);                          
                                                 
                     }); 
                 
               }) ; 
          });
                           

  }                                                      
    return deferred.promise;
}