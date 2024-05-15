import { join } from 'node:path';

import execa from 'execa';
import { globSync } from 'glob';
import gulp, { TaskFunction } from 'gulp';
import ts from 'gulp-typescript';
import { rimrafSync } from 'rimraf';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const scriptPath = './src/FileCabinet/SuiteScripts';
const scriptDest = () => gulp.dest(scriptPath);

export const clean: TaskFunction = (done) => {
    rimrafSync(`${scriptPath}/*.*`, { glob: { ignore: '.gitignore' } });
    done();
};

export const copy: TaskFunction = () => {
    return gulp.src('./public/**/*.*').pipe(scriptDest());
};

export const build: TaskFunction = () => {
    const project = ts.createProject('tsconfig.json');
    return gulp.src('./src/TypeScript/**/*.ts').pipe(project()).pipe(scriptDest());
};

export const upload: TaskFunction = (done) => {
    const { file } = yargs(hideBin(process.argv))
        .options({ file: { type: 'string' } })
        .parseSync();
    const fileList = globSync(file ? `**/${file}.js` : '**/*.js', { cwd: join(scriptPath, '..') });
    execa.sync('suitecloud', ['file:upload', '--paths', ...fileList.map((path) => join('/', path))], {
        stdout: process.stdout,
        stderr: process.stderr,
    });
    done();
};

export default gulp.series(clean, gulp.parallel(build, copy), upload);
