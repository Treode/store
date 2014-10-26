'use strict';

// The underscore on the file name ensures this file is listed first by globs.
angular.module ('treode', ['ng']);
;'use strict';

angular.module ('treode')

  .provider ('TreodeAuthorization', function() {
    var self = this;
    var $rootScope = null;
    var $q = null;

    self.redirectUrl = null;

    function param (src) {
      var s = '';
      for (var i in src) {
        s += encodeURIComponent (i);
        s += '=';
        s += encodeURIComponent (src [i]);
        s += '&';
      }
      return s.substring (0, s.length - 1);
    }

    function unparam (src) {
      var args = {};
      var strs = src.split ('&');
      for (var i in strs) {
        var s = strs [i];
        if (s.indexOf ('=') < 0) {
          args [decodeURIComponent (s)] = true;
        } else {
          var kv = s.split ('=');
          args [decodeURIComponent (kv [0])] = decodeURIComponent (kv [1]);
        }}
      return args;
    }

    self.authorities = {

      facebook: {

        prefix: 'facebook',

        // Verify at https://graph.facebook.com/app/?access_token=[token]
        authorizeUri: function (id, nonce, reauthorize) {
          return 'https://www.facebook.com/dialog/oauth?' + param ({
            client_id: id,
            redirect_uri: self.redirectUri + '?state=' + nonce,
            scope: 'email',
            response_type: 'token',
            display: reauthorize? 'none' : 'popup'
          });
        },

        // This just lands the user on the Facebook page, without logout.
        __logoffUri: function (token, redirect) {
          return 'https://www.facebook.com/logout.php?' + param ({
            next: redirect,
            access_token: token
          });
        },

        // Facebook appears to reject the CORS preflight request at the moment.
        __deauthorizeUri: function (token) {
          return 'https://graph.facebook.com/me/permissions?' + param ({
              access_token: token
          });
        }},

      google: {

        prefix: 'google',

        // Verify at https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=[token]
        authorizeUri: function (id, nonce, reauthorize) {
          return 'https://accounts.google.com/o/oauth2/auth?' + param ({
            client_id: id,
            redirect_uri: self.redirectUri,
            state: nonce,
            scope: 'https://www.googleapis.com/auth/userinfo.email ' +
                   'https://www.googleapis.com/auth/userinfo.profile',
            response_type: 'token'
          });
        },

        // To return later, when the Facebook stuff works too.
        __logoffUri: function (token, redirect) {
          return 'https://accounts.google.com/Logout';
        }}};

    function addWindowHook (provider, nonce) {
      window.completeAuth = function (search, hash) {
        $rootScope.$apply (function() {
          complete (provider, nonce, unparam (search), unparam (hash));
        });
      };
    }

    self.openWindow = function (uri) {
      window.open (uri, 'providerLogin', 'height=420,width=640');
    };

    self.clientIds = {};

    self.authorization = null;

    function begin (authority, reauthorize) {
      var provider = self.authorities [authority];
      if (!provider) throw new Error ('Unrecognized OAuth provider ' + authority);
      var id = self.clientIds [authority];
      if (!id) throw new Error ('No clientId for OAuth provider ' + authority);
      var nonce = Math .random() .toString (36) .substr (2,16);
      var uri = provider.authorizeUri (id, nonce, reauthorize);
      addWindowHook (provider, nonce);
      self.openWindow (uri);
    }

    function complete (provider, nonce, search, hash) {
      var state = search.state || hash.state || '';
      if (state != nonce) return;
      if (hash.access_token) {
        var details = {
          authority: provider,
          token: hash.access_token,
          expires: Date.now() + hash.expires_in
        };
        self.authorization = details;
        $rootScope.$broadcast ('authorizationChanged', details);
      } else {
        self.authorization = null;
        var normalize = {
          error: search.error || hash.error || 'unknown_error'
        };
        $rootScope.$broadcast ('authorizationChanged', normalize);
      }}

    self.selectAuthority = function (authority) {
      begin (authority, false);
    };

    self.$get = [
      '$rootScope', '$q',
      function (rs, http, q) {
        $rootScope = rs;
        $q = q;
        return self;
      }];

    return self;
  });
;'use strict';

angular.module ('treode')

  .factory ('TreodeResource', [
    '$http', '$parse', 'TreodeAuthorization',
    function ($http, $parse, auth) {

      var noop = angular.noop,
          forEach = angular.forEach,
          copy = angular.copy,
          getter = function (obj, path) {
            return $parse (path) (obj);
          };

      /**
       * We need our custom mehtod because encodeURIComponent is too aggressive and doesn't follow
       * http://www.ietf.org/rfc/rfc3986.txt with regards to the character set (pchar) allowed in
       * path segments:
       *    segment       = *pchar
       *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
       *    pct-encoded   = "%" HEXDIG HEXDIG
       *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
       *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
       *                     / "*" / "+" / "," / ";" / "="
       */
      function encodeUriSegment(val) {
        return encodeUriQuery(val, true).
          replace(/%26/gi, '&').
          replace(/%3D/gi, '=').
          replace(/%2B/gi, '+');
      }

      /**
       * This method is intended for encoding *key* or *value* parts of query component. We need a
       * custom method becuase encodeURIComponent is too agressive and encodes stuff that doesn't
       * have to be encoded per http://tools.ietf.org/html/rfc3986:
       *    query       = *( pchar / "/" / "?" )
       *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
       *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
       *    pct-encoded   = "%" HEXDIG HEXDIG
       *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
       *                     / "*" / "+" / "," / ";" / "="
       */
      function encodeUriQuery(val, pctEncodeSpaces) {
        return encodeURIComponent(val).
          replace(/%40/gi, '@').
          replace(/%3A/gi, ':').
          replace(/%24/g, '$').
          replace(/%2C/gi, ',').
          replace((pctEncodeSpaces ? null : /%20/g), '+');
      }

      // Route is very useful, and it would be cool to factor it from Angular.
      function Route(template, defaults) {
        this.template = template = template + '#';
        this.defaults = defaults || {};
        var urlParams = this.urlParams = {};
        forEach(template.split(/\W/), function(param){
          if (param && template.match(new RegExp("[^\\\\]:" + param + "\\W"))) {
            urlParams[param] = true;
          }
        });
        this.template = template.replace(/\\:/g, ':');
      }

      Route.prototype = {
        url: function(params) {
          var self = this,
          url = this.template,
          val,
          encodedVal;

          params = params || {};
          forEach (this.urlParams, function(_, urlParam) {
            val = params.hasOwnProperty(urlParam) ? params[urlParam] : self.defaults[urlParam];
            if (angular.isDefined(val) && val !== null) {
              encodedVal = encodeUriSegment(val);
              url = url.replace(new RegExp(":" + urlParam + "(\\W)", "g"), encodedVal + "$1");
            } else {
              url = url.replace(new RegExp("/?:" + urlParam + "(\\W)", "g"), '$1');
            }
          });
          url = url.replace(/\/?#$/, '');
          var query = [];
          forEach(params, function(value, key){
            if (!self.urlParams[key]) {
              query.push(encodeUriQuery(key) + '=' + encodeUriQuery(value));
            }
          });
          query.sort();
          url = url.replace(/\/*$/, '');
          return url + (query.length ? '?' + query.join('&') : '');
        }};

      return function (prefix, pattern, isUser) {
        if (!prefix)
          throw new Error ("Resource requires a prefix.");

        var obj = this || {};
        var route = pattern ? new Route (pattern) : null;

        function makeHeaders() {
          if (auth.authorization) {
            var details = auth.authorization;
            return {Authorization: "Bearer " + details.authority.prefix + "/" + details.token};
          } else {
            return {};
          }}

        obj.get = function (params, success, error) {
          if (!route || !params)
            throw new Error ("Resource.get requires a key pattern and parameters.");
          var value = {};
          $http.get (prefix + route.url (params), {headers: makeHeaders()})
            .then (function (response) {
              if (response.data)
                copy (response.data, value);
              (success || noop) (response);
            }, function (response) {
              (error || noop) (response);
            });
          return value;
        };

        if (isUser)
          obj.getme = function (success, error) {
            var value = {};
            $http.get (prefix + "/me", {headers: makeHeaders()})
              .then (function (response) {
                if (response.data)
                  copy (response.data, value);
                (success || noop) (response);
              }, function (response) {
                (error || noop) (response);
              });
            return value;
          };

        obj.list = function (success, error) {
          var value = [];
          $http.get (prefix, {headers: makeHeaders()})
            .then (function (response) {
              if (response.data) {
                value.length = 0;
                forEach (response.data, function (item) {
                  value.push (item);
                });
              }
              (success || noop) (response);
            }, function (response) {
              (error || noop) (response);
            });
          return value;
        };

        obj.search = function (query, success, error) {
          if (!query)
            throw new Error ("Resource.search requires a non-empty query.");
          var value = {};
          $http.get (prefix, {params: {q: query}, headers: makeHeaders()})
            .then (function (response) {
              if (response.data)
                copy (response.data, value);
              (success || noop) (response);
            }, function (response) {
              (error || noop) (response);
            });
          return value;
        };

        obj.post = function (data, success, error) {
          if (!data)
            throw new Error ("Resource.post requires data to post.");
          $http.post (prefix, data, {headers: makeHeaders()})
            .then (function (response) {
              (success || noop) (response.headers ("location"), response);
            }, function (response) {
              (error || noop) (response);
            });
        };

        obj.put = function (params, data, success, error) {
          if (!route || !params)
            throw new Error ("Resource.put requires a key pattern and parameters.");
          if (!data)
            throw new Error ("Resource.put requires data to send.");
          $http.put (prefix + route.url (params), data, {headers: makeHeaders()})
            .then (function (response) {
              (success || noop) (response.headers ("location"), response);
            }, function (response) {
              (error || noop) (response);
            });
        };

        if (isUser)
          obj.putme = function (data, success, error) {
            if (!data)
              throw new Error ("Resource.putme requires data to send.");
            $http.put (prefix + "/me", data, {headers: makeHeaders()})
              .then (function (response) {
                (success || noop) (response.headers ("location"), response);
              }, function (response) {
                (error || noop) (response);
              });
          };

        obj.remove = function (params, success, error) {
          if (!route || !params)
            throw new Error ("Resource.remove requires a key pattern and parameters.");
          // jshint -W024
          $http.delete (prefix + route.url (params), {headers: makeHeaders()})
            .then (function (response) {
              (success || noop) (response);
            }, function (response) {
              (error || noop) (response);
            });
        };

        if (isUser)
          obj.removeme = function (success, error) {
            $http.delete (prefix + "/me", {headers: makeHeaders()})
              .then (function (response) {
                (success || noop) (response);
              }, function (response) {
                (error || noop) (response);
              });
          };

        return obj;
      };
    }]);
