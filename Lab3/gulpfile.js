var gulp = require('gulp');
var sass = require('gulp-sass');
 
gulp.task('styles', function() {
    // copy my customized scss files to node_modules
    gulp.src('./scss/*.scss')
        .pipe(gulp.dest('./node_modules/bootstrap/scss/'));

    // copy compiled css to server
    gulp.src('node_modules/bootstrap/scss/*.scss')
        .pipe(sass().on('error', sass.logError))
        .pipe(gulp.dest('./public/css/'));
});

gulp.task('js', function() {
    gulp.src('./node_modules/bootstrap/dist/js/bootstrap.min.js')
        .pipe(gulp.dest('./public/js/'));
    gulp.src('./node_modules/jquery/dist/jquery.min.js')
        .pipe(gulp.dest('./public/js/'));
});

gulp.task('default',['styles', 'js']);