var ig = require('instagram-node').instagram();
var Q    = require('q');
var s_mongo_util = require('./utils');

ig.use({ access_token: '582742177.5b48709.62afa5bc923d4a638279fbdb39ef9575' });
ig.use({ client_id: '5b48709500c443c7bcfa190149322a70',
         client_secret: 'e46d5f7f32fd4a85a85b4a13cbd67ce6' });
exports.reformatHashIg = function(original_hash)
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




exports.find_all_igs = function(db_handle){
  console.log("find_all_igs::");
  var deferred = Q.defer();
  s_mongo_util.find_all_hashes(db_handle).then(function(hashtags){
      if(hashtags && hashtags.length>0){
          var r_hashtags=[];
          hashtags.forEach(function(t_){
              r_hashtags.push(s_mongo_util.reformatHash(t_.hash));
            
          });
          
          exports.find_selected_igs(r_hashtags,db_handle).then(function(igs_){
              deferred.resolve(igs_);
          });  
      }
    else{
      deferred.resolve([]);
    }
      
  });
  return deferred.promise;
}
exports.find_selected_igs = function(ig,db_handle){
  console.log("find_selected_igs::"+JSON.stringify(ig));
  if(ig && ig.length==1 && ig[0]=='all')
  {
    return exports.find_all_igs(db_handle);
  }
  else
  {
    
  var deferred = Q.defer();
  var tweet_value_regexp=[]
  if(ig&&ig.length>0)
    {
      var all_items=[];
        ig.forEach(function(item){
          var ig_key='instagram.'+item;
          var collection = db_handle.get(ig_key);
          console.log("Searching instagram "+ig_key );
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

exports.find_all_ig = function(db_handle){
  var deferred = Q.defer();
   var collection = db_handle.get('instagram');
    collection.find({},function(err,items){
            console.log("Find_all_instagram");
      if(items && items.length > 0){
          console.log("instagram items found "+items.length);
      }
            deferred.resolve(items);
       }); 
  return deferred.promise;
}
exports.find_selected_instagram = function(hashtag, db_handle)
{
   var deferred = Q.defer();
  if(hashtag)
   {
          var instagram_key = "instagram."+hashtag;
           var collection = db_handle.get(instagram_key);
           console.log("Searching "+instagram_key);
           collection.find({},function(err,items){
            console.log("Find_all_tweets");
            if(items && items.length > 0){
                console.log("Items found "+items.length);
             
            }
            else{
              console.log("No items found "+ instagram_key);
                
            }
              deferred.resolve(items);
           }); 
           
     
   }
    return deferred.promise;
}
exports.find_selected_instagrams = function(hashtags, db_handle)
{
      var deferred = Q.defer();
      var ig_searches = [];
//   exports.find_selected_instagram
    if(hashtags && hashtags.length>0)
     {
       hashtags.forEach(function(tag) {
               ig_searches.push(exports.find_selected_instagram(tag,db_handle));          
      });
       Q.all(ig_searches).then(function(instagrams){
         console.log("find_selected_instagrams::instagrams returned -->"+instagrams.length);
         deferred.resolve(instagrams);
       });
     } 
  return deferred.promise;  
}
exports.find_selected_igs = function(instagram_value,db_handle){
  if(instagram_value && instagram_value.length==1 && instagram_value[0]=='all')
  {
    return exports.find_all_ig(db_handle);
  }
  else
  {
      console.log("Find Instagrams "+JSON.stringify(instagram_value));
  var deferred = Q.defer();
  var ig_value_regexp=[]
  if(instagram_value && instagram_value.length>0)
    { 
        instagram_value.forEach(function(item){
        ig_value_regexp.push({"tags":{"$regex":item , "$options":'i'}});
      });
    }
    console.log("Regexp value  " +JSON.stringify(ig_value_regexp));
   var collection = db_handle.get('instagram');
 
  collection.find({  "$and" :
                     [ 
                       {"tags":{"$exists": true }  },
                       {"tags.0":{"$exists": true } },
                       {"$or":ig_value_regexp }
                     ]
                  },function(err,items){
            deferred.resolve(items);
       }); 
  
  return deferred.promise;
  }

}
exports.search_db_ig=function(cnt,hashtags,db_handle)
{
   console.log("search_db_ig");
  var deferred = Q.defer();//promise to return all data.
  if(hashtags)
  {
      var ht_r =[] ;
        hashtags.forEach(function(tag){
             ht_r.push(tag.substr(1));    
        }); 
        //exports.find_selected_igs(ht_r,db_handle)
        exports.find_selected_instagrams(ht_r,db_handle).then(function(mongo_data){ 
                        console.log("Instagram Mongo Data returned.");
                  
                            deferred.resolve(mongo_data); 
         });
  }                                                      
  return deferred.promise;
}
 
 
exports.add_stream_filter_ig=function(db_handle, hashtag)
{
  if(db_handle && hashtag)
  {
        console.log("Reformatting hashtag.");
        var reformatted_hashtag_ = '';
        if(hashtag && hashtag.hash)
        {
              reformatted_hashtag_= hashtag.hash;
        }
        else
        {
              reformatted_hashtag_= hashtag;
        }
        var collection=db_handle.get("tweet");
        console.log("Adding stream filter to "+ reformatted_hashtag_);
        console.log("Hashtag is : "+JSON.stringify(hashtag));
       var stream_= T.stream('statuses/filter', { track:hashtag }, function(stream)
        {
          console.log("Stream activated.");
        });
        stream_.on('tweet',function(t){
          console.log("Tweet: "+JSON.stringify(t));
          collection.insert(t);
        });
        console.log("Steam for reformatted_hashtag_=="+reformatted_hashtag_);
  } 
}
exports.search_ig=function( cnt , hashtag  )
{
  
  var reformatted_hashtag_  = exports.reformatHashIg(hashtag);
  
   var deferred = Q.defer();
    ig.tag_media_recent(reformatted_hashtag_, function(err, medias, pagination, remaining, limit) { 
     
      deferred.resolve(medias);
    });
    return  deferred.promise;
}
exports.transform_ig=function(hashtags,post)
{
   console.log("transform_ig");
  var json_obj = {};
  var media_array=[];
  var used_hashtags =[];
  var deferred = Q.defer();  
  if(post)
  {   
       
           json_obj.gen_url= post.link; 
          if(post.created_time)
          {
              json_obj.created_at=new Date(post.created_time*1000);
          }
          json_obj.media=media_array;  
          json_obj.type='instagram.search'; 
          if(post.user)
          { 
              json_obj.screen_name=post.user.username;
              json_obj.name=post.user.full_name;
              json_obj.user_id=post.user.id;
              json_obj.user_id_str = ""+post.user.id;

          } 
          if(post.caption)
          {  
               json_obj.text_str=post.caption.text; 
               json_obj.id=post.caption.id;
               json_obj.id_str=""+post.id;
          }
          json_obj.hashtags = used_hashtags;
          json_obj.media=media_array;
        if(post.tags)
         {    
           post.tags.forEach(function(tag_){
               json_obj.hashtags.push(tag_);
           });
                            
          }    
          if(post.images)
          { 
                  var media_item_={};
                  media_item_.social_url=post.link;
                  media_item_.image_url=post.images.low_resolution.url; 
                         if(post.images.low_resolution)
                         {
                           media_item_.small_image_url=post.images.low_resolution.url;
                           media_item_.small_image_width=post.images.low_resolution.width;
                           media_item_.small_image_height=post.images.low_resolution.height;
                         }
                        if(post.images.standard_resolution)
                         {
                          
                           media_item_.large_image_url=post.images.standard_resolution.url;
                           media_item_.large_image_width=post.images.standard_resolution.width;
                           media_item_.large_image_height=post.images.standard_resolution.height;
                         }
                        if(post.images.thumbnail)
                         {
                            media_item_.thumb_image_url=post.images.thumbnail.url;
                            media_item_.thumb_image_width=post.images.thumbnail.width;
                            media_item_.thumb_image_height=post.images.thumbnail.height
                         } 
                          json_obj.media.push(media_item_);                 
            } 
      deferred.resolve(json_obj); 
    } 
  else
  {
    console.log("transform_ig - > No tweet found.") ;   
  }
  return deferred.promise;
}
exports.insert_post=function(instagram_post,hashtags,max_rollover,db_handle)
{
  
}

exports.search_multiple_ig=function(cnt, hashtags,db_handle)
{
  console.log("search_multiple_ig::");
  var deferred = Q.defer();//promise to return all data.
  var collection=db_handle.get("instagram");
  if(hashtags)
    {
      var queue_of_tasks = [];
      var queue_of_parse_tasks = [];
      var ig_ids=[];
      var mongo_aa={};
      hashtags.forEach(function(hashtag)
      {
       // console.log("Instagram Searching "+hashtag);
        queue_of_tasks.push(  exports.search_ig( cnt,hashtag ));  
        queue_of_tasks.push(  exports.search_db_ig( cnt, [hashtag],db_handle ));  
      });
         Q.all(queue_of_tasks).then(function(igs){ 
           if(igs && igs.length>0)
           { 
             igs.forEach(function(ig)
             {
               if(ig && ig.length>0)
               { 
                 ig.forEach(function(instagram_){
                   if(instagram_._id){ 
                     mongo_aa[instagram_.id]=instagram_;
                   }
                   else{
                     console.log("Instagram non-mongo item ");
                      
                   }
                   
                 });
               }
             }) ;
           igs.forEach(function(ig)
                       {
                         if(ig && ig.length>0)
                         { 
                           ig.forEach(function(instagram_){
                             if(instagram_._id)
                             {
                                console.log("Mongo Instagram push item ");
                                queue_of_parse_tasks.push(  exports.transform_ig(hashtags,instagram_)  );
                             }
                             else
                             {
                               console.log("Instagram non-mongo item ");
                               var stored_=mongo_aa[instagram_.id];
                               if(!stored_)
                               { 
                                 console.log("Inserting instagram pushing "+instagram_.id);
                                // collection.insert(instagram_);
                                 s_mongo_util.insert_instagram(  hashtags, instagram_ );
                                 queue_of_parse_tasks.push(  exports.transform_ig(hashtags,instagram_)  );

                               }
                               else{
                                   console.log("Instagram message is already in db "+instagram_.id);
                               }
                             }    
                         });
                       } 
                     });    
           }
         }).then(function(){
           
           
            Q.all(queue_of_parse_tasks).then(function(ful_data){ 
                                deferred.resolve(ful_data);
                            });
         });
           
    }
  
   return deferred.promise;       
}
