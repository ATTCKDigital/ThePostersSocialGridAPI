// call the packages we need
var express    = require('express'); 		// call express
var ntwitter = require('ntwitter');
var bodyParser = require('body-parser');
var Q =          require('q');
var logger = require('morgan');
var cookieParser = require('cookie-parser');

var _ = require("underscore");
var mongo = require('mongodb');
var monk = require('monk');
var db = monk('mongodb://tpapi:tpapi@linus.mongohq.com:10002/app29194626');
var mubsub = require('mubsub');
var client = mubsub('mongodb://tpapi:tpapi@linus.mongohq.com:10002/app29194626');



var s_twitter = require('./s_twitter');
var s_instagram = require('./s_instagram');
var utils = require('./utils');
var app        = express(); 				// define our app using express


//application context to use in this application
var application_context='/theposters';


//port to bind to for this application
var port = process.env.PORT || 8080; 		// set our port

var CURRENT_HASHES_=[];

// configure app to use bodyParser()
// this will let us get the data from a POST
app.use(bodyParser.urlencoded({ extended: true }));
app.enable("jsonp callback"); 
app.use(function(req,res,next){
    req.db = db;
    req.mongo_client=client;
    next();
});

findHashesInRequestCount= function(requested_hashes, response_hashes)
{
   var deferred = Q.defer();
  var count_ = 0;
  if(requested_hashes && response_hashes && requested_hashes.length> 0 && response_hashes.length > 0 )
   {
     console.log("Requested :"+requested_hashes);
     console.log("Responses :"+response_hashes);
 
     requested_hashes.forEach(function(hash){
       var index_=searchStringInArray(hash,response_hashes).then(function(index_){
           console.log("Index is "+index_+" for "+hash+"  "+ response_hashes);
           if(index_>-1)
           {
             count_++;
           }
       }) ;
       
     }); 
   }
  deferred.resolve(count_);
  return deferred.promise; 
}
searchStringInArray=function(str, strArray) {
   var deferred = Q.defer();
    for (var j=0; j<strArray.length; j++) {
      if (strArray[j].match(str)) { 
        deferred.resolve(j);
        return j;
      }
    }
    deferred.resolve(-1); 
    return deferred.promise;
}
app.use(bodyParser.json());
rank = function(conf_obj)
{  
  var data=conf_obj.data;
  var date_sort_asc = conf_obj.date_sort_asc;
  var rank_sort_asc = conf_obj.rank_sort_asc;
  var hashtag_array = [] ;
  if(conf_obj.hashtags){ 
      conf_obj.hashtags.forEach(function(tag){ 
        hashtag_array.push(utils.reformatHash(tag));
      });  
  }  
   var deferred = Q.defer();//promise to return all data.
   if(data.length>0 && (!data[0].gen_url))
     {
       var new_data=[];
       data.forEach(function(d){ 
           if(d && d.id)
             { 
               new_data.push(d);
             }
             else if(d && d.length > 0)
             {
                d.forEach(function(nd){  
                   new_data.push(nd);
                 });
             }
          
       });
       data=new_data;
     }
    console.log("Sorting ... ");
   data.sort(function(tweet1,tweet2){
                                  var d1 = new Date(tweet1.created_at);
                                  var d2 = new Date(tweet2.created_at);
                                 if(date_sort_asc)
                                 {
                                     return d1-d2;
                                 } 
                                 return d2-d1;
                            });
  var LOAD_WEIGHT_HASHTAG = 10000000000000;
  var LOAD_WEIGHT_MEDIA= 10000000000;
  var LOAD_WEIGHT_TEXT=  100000000;
  var rank_value_date=data.length;
 
  data.forEach(function(tweet){ 
        tweet.ranking =rank_value_date;
        if(tweet.text_str)
        {   
            tweet.ranking=tweet.ranking+LOAD_WEIGHT_TEXT;    
        } 
        if(tweet.media)
        {  
           tweet.ranking=tweet.ranking+(LOAD_WEIGHT_MEDIA*tweet.media.length );
        } 
        if(tweet.hashtags && hashtag_array)
        { 
       
          var intersection_ = _.intersection(tweet.hashtags,hashtag_array);
 
          tweet.matching_hashtags=intersection_; 
          tweet.ranking=tweet.ranking+(LOAD_WEIGHT_HASHTAG*tweet.matching_hashtags.length);   
        } 
        rank_value_date--;
  }); 
  data.sort(function(t1,t2){
      if(rank_sort_asc)
      {
              return t1.ranking-t2.ranking;
      }
      return t2.ranking-t1.ranking;
  });
  deferred.resolve(data);
  return deferred.promise;
}
save_all_hashes=function(db_handle, hashes_)
{
  var init_stream_=false;
  var hash_array_reformatted_=[];
  if(hashes_ && hashes_.length>0)
  {
    hashes_.forEach(function(hash_){
        hash_array_reformatted_.push(utils.reformatHash(hash_));
    });
  }
  console.log("save_all_hashes "+JSON.stringify(hashes_));
   var saved_hash_arr_=[];
   saved_hash_arr_.push(utils.find_all_hashes(db_handle));
    Q.all(saved_hash_arr_).then(function(all_saved_hashes_){
       console.log("Found hashes :" +JSON.stringify(all_saved_hashes_));
        var existing_hashes=[];
        var new_hashes_to_save_=[];
        var collection = db_handle.get('search_hashes');
        if(all_saved_hashes_ && all_saved_hashes_.length>0)
        {
          all_saved_hashes_.forEach(function(hash_arr){
            if(hash_arr && hash_arr.length>0)
              {
                hash_arr.forEach(function(hash){
                    console.log("Hash is "+JSON.stringify(hash));
                    var reformatted_hash_ = utils.reformatHash(hash.hash) ;
                    existing_hashes.push(reformatted_hash_); 
                });
                if(CURRENT_HASHES_.length==0)
                {
                  console.log("Current Hashes is empty.");
                   existing_hashes.forEach(function(hash_s_){
                       console.log("All saved hashes being pushed in.");
                       console.log("Hash : "+JSON.stringify(hash_s_));
                       CURRENT_HASHES_.push( hash_s_);
                       init_stream_=true;
                     });
                }
              }
           
          });
        } 
      console.log("Existing hashes MongoDB : "+JSON.stringify(existing_hashes));
      console.log("Hashes : "+JSON.stringify(hash_array_reformatted_));
      var _difference_in_hashes = _.difference(hash_array_reformatted_,existing_hashes); 
      console.log("Differences Are "+JSON.stringify(_difference_in_hashes));
         if(_difference_in_hashes && _difference_in_hashes.length>0)
         {
             _difference_in_hashes.forEach(function(hash_){
               var twitter_key ="twitter."+hash_;
               var instagram_key = "instagram."+hash_;
               console.log("Creating "+twitter_key+" "+instagram_key);
               // var channel = client.channel( twitter_key , { size: 5000000, max: 5000 });
               // var channel = client.channel( instagram_key , { size: 5000000, max: 5000 });
//                utils.create_channels(hash_).then(function(channel_array){
                 console.log("Completing Creating "+twitter_key+" "+instagram_key);   
                CURRENT_HASHES_.push( hash_);
                init_stream_=true;
                 var hash_obj_ = {hash: "#"+hash_};
                 collection.insert(hash_obj_) ; 
                 console.log("Stored Hash "+hash_);
             
               
               }); 
         }
         else
         {
            console.log("No action, no differences.");   
         }
         if(init_stream_)
         { 
                 console.log("Listening to new filters....");
                 s_twitter.add_stream_filter(CURRENT_HASHES_);
                 console.log("After adding a stream.");
         }
    });
 
}
delete_hash=function(db_handle,name_)
{
  console.log("delete_hash --> "+name_);
    var deferred = Q.defer();
    var collection = db_handle.get('search_hashes');
    var hash_obj_ = {hash: "#"+name_};
    collection.remove(hash_obj_,function(err){
      if(err)
      {
          deferred.resolve(err);
      }
      else
      {
          deferred.resolve("Succesfully deleted : "+JSON.stringify(hash_obj_));
      }
    });
    return deferred.promise;
} 
search_social_media = function(db_handle, client,  count_, hashes_)
{
    var deferred = Q.defer();//promise to return all data.
    var searches_ = [];
  
    searches_.push(  s_twitter.search_multiple(  100 , hashes_ , db_handle, client ) );
    searches_.push(  s_instagram.search_multiple_ig(  100 , hashes_  , db_handle ) );
    save_all_hashes(db_handle, hashes_);
     Q.all(searches_).then(function(data){ 
                           var media_item_array_ =[];
                                  data.forEach(function(item_){
                                      item_.forEach(function(i_){  
                                        console.log("Media pushing "+i_.type);
                                        console.log(JSON.stringify(i_));
                                            media_item_array_.push(i_);
                                      });
                                     console.log("ITEM_ IS "+JSON.stringify(item_)+" "+item_.length);
                                 });
       console.log("Media Item Array :: Search :: "+JSON.stringify(media_item_array_));
                            var conf_obj = {data: media_item_array_ , date_sort_asc: false,  hashtags: hashes_, rank_sort_asc: false};
         
                            rank(conf_obj).then(function(data_ranked){ 
                                console.log("Data Ranked is "+JSON.stringify(data_ranked));
                              deferred.resolve(media_item_array_); 
                           }); 
                           
                      }); 
      return deferred.promise;
}
// ROUTES FOR OUR API
// =============================================================================
var router = express.Router(); 				// get an instance of the express Router

// test route to make sure everything is working (accessed at GET http://localhost:8080/api)
router.get('/', function(req, res) {
	res.jsonp({ message: ' welcome to '+application_context+' root api!' });	
});

router.get('/socialmedia/hash/find/all', function(req, res) {
 var saved_hash_arr_=[];
   saved_hash_arr_.push(utils.find_all_hashes(req.db));
    Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp({ message: ' saved hashes', hashes: ful });	
    });
});
router.get('/socialmedia/tweets/find/:tweets', function(req, res) {
 var saved_hash_arr_=[];
    var tweets_ = req.params.tweets;
    var array_values_ = tweets_.split(",");
   saved_hash_arr_.push(s_twitter.find_selected_tweets(array_values_,req.db));
    Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp({ message: ' saved tweets', hashes: ful });	
    });
});
router.get('/socialmedia/data/find/:vals', function(req, res) {
 var saved_hash_arr_=[];
    var vals_ = req.params.vals;
    var array_values_ = vals_.split(",");
   saved_hash_arr_.push(s_twitter.find_selected_tweets(array_values_,req.db));
   saved_hash_arr_.push(s_instagram.find_selected_igs(array_values_,req.db));
    Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp({ message: ' saved data ', hashes: ful });	
    });
});
router.get('/socialmedia/instagrams/find/:instagrams', function(req, res) {
 var saved_hash_arr_=[];
    var instagrams_ = req.params.instagrams;
    var array_values_ = instagrams_.split(",");
   saved_hash_arr_.push(s_instagram.find_selected_igs(array_values_,req.db));
    Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp({ message: ' saved instagrams', hashes: ful });	
    });
});
router.get('/socialmedia/tweets/find/all', function(req, res) {
 var saved_hash_arr_=[];
   saved_hash_arr_.push(s_twitter.find_all_tweets(req.db));
    Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp({ message: ' saved tweets', hashes: ful });	
    });
});
router.get('/socialmedia/hash/remove/:hash', function(req,res){
  console.log("Remove hash "+req.params.hash);
  var saved_hash_arr_=[];
  saved_hash_arr_.push(delete_hash(req.db,req.params.hash));
  Q.all(saved_hash_arr_).then(function(ful){
      res.jsonp(ful);
  });
});
router.get('/socialmedia/:hashes', function(req,res){
//   res.json({message:'twitter default message'});
  console.log(application_context+'/socialmedia'); 
  var date_sort_asc_=false;
  var rank_sort_asc_=false;
  
  var mod_hashtags=[];
  var hashes_ =[];
  if(req.params.hashes){
      
      hashes_ = req.params.hashes.split(",");
      hashes_.forEach(function(hash){ 
      var reformatted_=utils.reformatHash(hash); 
            mod_hashtags.push(reformatted_);
      }); 
    
     //search social media
    search_social_media(db,client, 100,hashes_).then(function(full_data){
            console.log("Promises fulfilled -- twitter/instagram");
          res.jsonp(full_data);//return all data
    });
  }
  else {
      console.log('no hashes found.');
    }
 
//s_twitter search hashtag here

});

// more routes for our API will happen here

// REGISTER OUR ROUTES -------------------------------
// all of our routes will be prefixed with /api
app.use(application_context, router);

// START THE SERVER
// =============================================================================
app.listen(port);
console.log('The Posters Listening on port --> ' + port+ '  application context '+application_context);