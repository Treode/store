/* Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

angular.module ("movies")

  .controller ("MovieCtrl", [
    "$scope", "$location", "$routeParams", "Movie",
    function ($scope, $location, $routeParams, Movie) {

      var noop = angular.noop;
      var backup = null;

      var created = function (location, response) {
        if (location)
          $location.url (location);
      };

      var removed = function (response) {
        $location.url ("/");
      };

      var error = function (response) {
        $scope.$emit ("raiseAlert", {response: response});
      };

      $scope.newCast = {};
      $scope.newCastActorId = "";

      if ($routeParams.movieId) {
        $scope.movie = Movie.get ({movieId: $routeParams.movieId}, noop, error);
        $scope.editing = false;
      } else {
        $scope.movie = {cast: []};
        $scope.editing = true;
      }

      $scope.edit = function() {
        backup = $scope.movie;
        $scope.editing = true;
      };

      $scope.remove = function() {
        Movie.remove ({movieId: $scope.movie.id}, removed, error);
      };

      $scope.save = function() {
        if ($scope.movie.id)
          Movie.put ({movieId: $scope.movie.id}, $scope.movie, created, error);
        else
          Movie.post ($scope.movie, created, error);
        $scope.editing = false;
      };

      $scope.cancel = function() {
        if (backup) {
          $scope.movie = backup;
        } else {
          $location.url ("/");
        }
        $scope.editing = false;
      };

      $scope.suggestActor = function (index) {
        return {
          placeholder: "Actor",
          minimumInputLength: 3,
          ajax: {
            url: store + "/actor",
            data: function (term, page) {
              return {q: term};
            },
            results: function (data, page) {
              return {
                results: data.actors || []
              };
            }},
          formatResult: function (data, element, query) {
            return data.actor || data.name;
          },
          formatSelection: function (data, element) {
            var cast = index == -1 ? $scope.newCast : $scope.movie.cast [index];
            cast.actorId = data.actorId || data.id;
            cast.actor = data.actor || data.name;
            return cast.actor;
          },
          initSelection: function (element, callback) {
            callback ($scope.movie.cast [index]);
          }};
      };

      $scope.addCast = function() {
        var r = $scope.newCast;
        $scope.movie.cast.push (r);
        $scope.newCast = {};
        $scope.newCastActorId = "";
      };

      $scope.removeCast = function (index) {
        $scope.movie.cast.splice (index, 1);
      };
    }]);
