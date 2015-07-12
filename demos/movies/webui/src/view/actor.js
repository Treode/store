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

  .controller ("ActorCtrl", [
    "$scope", "$location", "$routeParams", "Actor",
    function ($scope, $location, $routeParams, Actor) {

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

      $scope.newRole = {};
      $scope.newRoleMovieId = "";

      if ($routeParams.actorId) {
        $scope.actor = Actor.get ({actorId: $routeParams.actorId}, noop, error);
        $scope.editing = false;
      } else {
        $scope.actor = {roles: []};
        $scope.editing = true;
      }

      $scope.edit = function() {
        backup = $scope.actor;
        $scope.editing = true;
      };

      $scope.remove = function() {
        Actor.remove ({actorId: $scope.actor.id}, removed, error);
      };

      $scope.save = function() {
        if ($scope.actor.id)
          Actor.put ({actorId: $scope.actor.id}, $scope.actor, noop, error);
        else
          Actor.post ($scope.actor, created, error);
        $scope.editing = false;
      };

      $scope.cancel = function() {
        if (backup) {
          $scope.actor = backup;
        } else {
          $location.url ("/");
        }
        $scope.editing = false;
      };

      $scope.suggestMovie = function (index) {
        return {
          placeholder: "Movie",
          minimumInputLength: 3,
          ajax: {
            url: store + "/movie",
            data: function (term, page) {
              return {q: term};
            },
            results: function (data, page) {
              return {
                results: data.movies || []
              };
            }},
          formatResult: function (data, element, query) {
            return data.title;
          },
          formatSelection: function (data, element) {
            var role = index == -1 ? $scope.newRole : $scope.actor.roles [index];
            role.movieId = data.movieId || data.id;
            role.title = data.title;
            return data.title;
          },
          initSelection: function (element, callback) {
            callback ($scope.actor.roles [index]);
          }};
      };

      $scope.addRole = function() {
        var r = $scope.newRole;
        $scope.actor.roles.push (r);
        $scope.newRole = {};
        $scope.newRoleMovieId = "";
      };

      $scope.removeRole = function (index) {
        $scope.actor.roles.splice (index, 1);
      };
    }]);
