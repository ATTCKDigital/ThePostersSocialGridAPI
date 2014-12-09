var CommonModules
{ 
  ntwitter    : require('ntwitter');
  bodyParser  : require('body-parser');
  Q           : require('q');
  s_twitter   : require('./s_twitter');
  s_instagram : require('./s_instagram');
  logger      : require('morgan');
  cookieParser = require('cookie-parser');
//application context to use in this application 
  _           : require("underscore");
  mongo       : require('mongodb');
  monk        : require('monk');
};
module.exports=CommonModules;