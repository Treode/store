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

module.exports = function (grunt) {

  grunt.initConfig ({

    pkg: grunt.file.readJSON ('package.json'),

    copy: {
      dev: {
        files: [{
          expand: true,
          cwd: 'src/',
          src: ['**/*.{css,js}'],
          dest: 'dev/'
        },
        {
          expand: true,
          cwd: 'nocdn/',
          src: ['**/*.{css,js}'],
          dest: 'dev/nocdn/'
        }]
      },
      dist: {
        files: [{
          expand: true,
          cwd: 'nocdn/',
          src: ['**/*.{css,js}'],
          dest: 'dist/nocdn/'
        }]
      }
    },

    cssmin: {
      dist: {
        files: {
          'dist/<%= pkg.name %>.min.css': ['src/movies.css']
        }}},

    jade: {
      dev: {
        options: {
          pretty: true
          },
        files: [{
          expand: true,
          cwd: 'src/',
          src: ['**/*.jade'],
          dest: 'dev/',
          ext: '.html'
        }]
      },
      dist: {
        options: {
          data: {
            prod: true
          }},
        files: [{
          expand: true,
          cwd: 'src/',
          src: ['**/*.jade'],
          dest: 'dist/',
          ext: '.html'
        }]
      }},

    jshint: {
      files: ['src/**/*.js'],
      options: {
        browser: true,
        globalstrict: true,
        globals: {
          angular: true,
          store: true
        },
        jquery: true
      }},

    concat: {
      options: {
        separator: ';'
      },
      dist: {
        src: ['src/**/*.js'],
        dest: 'dist/<%= pkg.name %>.js'
      }},

    uglify: {
      dist: {
        files: {
          'dist/<%= pkg.name %>.min.js': ['<%= concat.dist.dest %>']
        }}},

    watch: {
      script: {
        files: ['src/**/*.{css,jade,js}'],
        tasks: ['jshint', 'copy:dev', 'jade:dev']
      }
    },

    clean: {
      dev: ['dev'],
      dist: ['dist']
    }});

  grunt.loadNpmTasks ('grunt-contrib-clean');
  grunt.loadNpmTasks ('grunt-contrib-concat');
  grunt.loadNpmTasks ('grunt-contrib-copy');
  grunt.loadNpmTasks ('grunt-contrib-cssmin');
  grunt.loadNpmTasks ('grunt-contrib-jade');
  grunt.loadNpmTasks ('grunt-contrib-jshint');
  grunt.loadNpmTasks ('grunt-contrib-uglify');
  grunt.loadNpmTasks ('grunt-contrib-watch');


  // The "dev" task uses readable JS and a local server
  grunt.registerTask ('dev', ['jshint', 'copy:dev', 'jade:dev']);

  // The "dist" uses minified JS/CSS and the production server
  grunt.registerTask ('dist',
    ['jshint', 'copy:dist', 'cssmin:dist', 'jade:dist', 'concat:dist', 'uglify:dist']);

  grunt.registerTask ('default', ['dist']);
};
