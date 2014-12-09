var mubsub = require('mubsub');
var client = mubsub('mongodb://tpapi:tpapi@linus.mongohq.com:10002/app29194626');
var MONGO_MAX_SIZE=5000000;
var MONGO_MAX_MSGS=5000;
var Q =          require('q');
var MONGO_CHANNELS={};
exports.get_client=function()
{
  return client;
}

exports.reformatHashes = function(hashtags){
  var tags=[];
  if(hashtags){
    hashtags.forEach(function(tag){
        tags.push(exports.reformatHash(tag));
    });
  }
  return tags;
}
exports.reformatTweetHashes = function(tweet){
  var tags=[];
  if(tweet && tweet.entities && tweet.entities.hashtags){
    tweet.entities.hashtags.forEach(function(tag){
        tags.push(exports.reformatHash(tag.text));
    });
  }
  return tags;
}
exports.reformatHash = function(original_hash)
{
  var new_hash = original_hash;

        if(new_hash && new_hash.length>0){
          if(new_hash.indexOf("#")>-1)
          {
            new_hash = new_hash.substr(new_hash.indexOf("#")+1);
          }
          new_hash=new_hash.toLowerCase();
        }  
  return new_hash;
}
exports.get_db_ids=function(tweets)
{
 
  var mongo_aa={};
   var deferred = Q.defer();
  if(tweets && tweets.length > 0)
  {
      var c=0;
            tweets.forEach(function(md){ 
              if(md.statuses)
              { 
                  md.statuses.forEach(function(data_item_){
                    if(data_item_._id)
                    {  
                      mongo_aa[data_item_.id]=data_item_;
                    } 
                    else{
                      console.log("get_db_ids::(1)Twitter item not in mongo -->"+data_item_.id);
                    }  
                } );  
              }
              else{
                
                if(!md['dummy'])
                  {
                      if(md['_id'])
                        {
                          
                          mongo_aa[md['message']['id']]=md['message']; 
                        } 
                  } 
                } 
            }); 
            deferred.resolve(mongo_aa);
  }
  else
  {
    console.log("get_db_ids::No tweets were found.");  
    deferred.resolve({});
  }
  return deferred.promise;
}
exports.insert_tweet_db=function(tweet,hash){
  var tweet_val = tweet;
  if(tweet['message'])
    {
      tweet_val=tweet['message'];
    } 
  if(tweet)
    {
     var channel_= exports.get_twitter_channel(hash);
      if(channel_)
       {
           channel_.publish("twitter."+hash,tweet_val);
       }
       else
       {
              console.log("channel - 2 - insert twitter."+hash+" undefined.");
       }
      
    }
     else
       {
         console.log("insert_tweet_db::Error, tweet undefined.");
       }
  
}
exports.insert_instagram_db=function(instagram,hash){ 
  var instagram_val = instagram;
  if(instagram_val['message'])
    {
      instagram_val=instagram_val['message'];
    } 
  if(instagram_val)
    { 
        var hash_ = exports.reformatHash(hash); 
      if(instagram_val.tags && instagram_val.tags.length>0)
      {
       instagram_val.tags.forEach(function(tag){
         var reformatted_tag_ = exports.reformatHash(tag);
         if(reformatted_tag_==hash_)
         { 
              var channel=exports.get_instagram_channel(hash_) ;
                if(channel)
                 { 
                     channel.publish("instagram."+hash_,instagram_val); 
                 }
                 else
                 {
                        console.log("channel - 2 - insert instagram."+hash_+" undefined.");
                 }
              
         }
       }); 
      
       
          
      }
    }
    else
    {
         console.log("insert_instagram_db::Error, tweet undefined.");
    }
  
}
exports.create_channels=function(hash)
{
  var channels_=[];
   var deferred = Q.defer();
  
  channels_.push(exports.get_instagram_channel(hash));
  channels_.push(exports.get_twitter_channel(hash));
  deferred.resolve(channels_);
  
  return deferred.promise;
}
exports.get_instagram_channel=function(hash){
    var channel = null;  
  if(hash)
    {
      var channel =  MONGO_CHANNELS["instagram."+hash];
      
       if(!channel)
       {  
              channel = client.channel( "instagram."+hash ,  { size: MONGO_MAX_SIZE, max: MONGO_MAX_MSGS }); 
              MONGO_CHANNELS["instagram."+hash]=channel; 
       }  
    } 
    else
      {
        console.log("get_instagram_channel::"+hash+" was   found - returning");
      } 
    return channel;
}
exports.find_all_hashes=function(db_handle, hashes_)
{
  var deferred = Q.defer();//promise to return all data.
  
  console.log("find_all_hashes -->"+JSON.stringify(hashes_));
   var collection = db_handle.get('search_hashes'); 
  if(hashes_)
    {
       collection.find(hashes_,function(err,items){
         if(items){
           deferred.resolve(items);
         }
         else{ 
           deferred.reject(err);
         }
       }); 
    }
    else
    {
      collection.find({},function(err,items){
        if(items){
          deferred.resolve(items);
        }
         
       }); 
    }  
  return deferred.promise;
}
exports.get_twitter_channel=function(hash){
    var channel = null;  
  if(hash)
    {
      var channel =  MONGO_CHANNELS["twitter."+hash];
       if(!channel)
       { 
             channel = client.channel( "twitter."+hash,  { size: MONGO_MAX_SIZE, max: MONGO_MAX_MSGS });  
             MONGO_CHANNELS["twitter."+hash]=channel;
       }  
    } 
  return channel;
}
exports.insert_twitter=function( tweets,stored_hashtags, mongo_aa)
{
  var deferred = Q.defer();
  var tweets_to_transform = []; 

      if(tweets && tweets.length>0) 
      { 
                             tweets.forEach(function(status){  
                                 var mongo_value = mongo_aa[status.id];
                                 if(mongo_value){ 
                                   if(status._id){
                                       tweets_to_transform.push(status);
                                   }
                                 } 
                                 else
                                   { 
                                      tweets_to_transform.push(status); 
                                     var twitter_hashes = exports.reformatTweetHashes(status);
                                     var reformatted_hashes =   exports.reformatHashes(stored_hashtags);
                                     twitter_hashes.forEach(function(twitter_hash){
                                         reformatted_hashes.forEach(function(stored_hash){
                                            
                                           if(stored_hash==twitter_hash){ 
                                              var mongo_value = mongo_aa[status.id];
                                             if(mongo_value){
                                               console.log("insert_twitter::twitter "+status.id+" already in db.");
                                             }
                                             else
                                              {
                                                 console.log("insert_twitter::twitter "+status.id+" being inserted  in db.");
                                                 exports.insert_tweet_db(status,stored_hash);
                                              } 
                                           }
                                         });
                                     });
                                          
                                   }
                                 
                             });
                            deferred.resolve(tweets_to_transform);
        
      }
      else
      { 
          deferred.resolve([]);
      }
  return deferred.promise;
}
exports.insert_instagram=function(  stored_hashtags, instagram )
{
  var deferred = Q.defer();
  var ig_to_transform = []; 
  if(stored_hashtags && stored_hashtags.length>0)
  {
    stored_hashtags.forEach(function(hashtag){
        exports.insert_instagram_db(instagram,hashtag);
    });
  }
  return deferred.promise;
}

