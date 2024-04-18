import gulp, { TaskFunction } from 'gulp';
import ts from 'gulp-typescript';
import { rimrafSync } from 'rimraf';

const dest = `./src/FileCabinet/SuiteScripts`;

export const clean: TaskFunction = (done) => {
    rimrafSync(`${dest}/*`, { glob: { ignore: '.gitignore' } });
    done();
};

export const build: TaskFunction = () => {
    const project = ts.createProject('tsconfig.json');
    return gulp.src('./src/TypeScript/!(*.d).ts').pipe(project()).pipe(gulp.dest(dest));
};

export const copy: TaskFunction = () => {
    return gulp.src('./public/*.*').pipe(gulp.dest(dest));
};

export default gulp.series(clean, gulp.parallel(build, copy));
