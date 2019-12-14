const { series, parallel } = require('gulp');

const gulp = require('gulp');
const ts = require('gulp-typescript');
const mocha = require('gulp-mocha');
const gulp_clean = require('gulp-clean');
const runSequence = require('run-sequence');
const path = require('path');

exports.build = series(
  clean,
  parallel(
    build,
    copyconfigfiles
  )
);
exports.clean = clean;
exports.copyconfigfiles = copyconfigfiles;
exports.test = test;
exports.default = series(
  clean,
  parallel(
    build,
    copyconfigfiles
  )
);

const tsProject = ts.createProject('./tsconfig.json');

function build(cb) {
  const merge = require('merge2');

  var tsResult = tsProject.src()
      .pipe(tsProject());


  return merge([
    tsResult.dts.pipe(gulp.dest('./definitions')),
    tsResult.js.pipe(gulp.dest(tsProject.config.compilerOptions.outDir))
  ]);
}

function clean(cb) {
  return gulp.src('dist/**/*', { read: false })
    .pipe(gulp_clean());
}

function test(cb) {
  return gulp.src('./dist/test/**/*.ts')
    .pipe(tsProject())
    .pipe(mocha( { require: ['ts-node/register'] } ));
}

function copyconfigfiles(cb) {
  const merge = require('merge2');
    
  return merge([
    gulp.src('./config/**/*.json')
      .pipe(gulp.dest('./dist/config/')),
    gulp.src('./package.json')
      .pipe(gulp.dest('./dist/'))
    ]);
}

