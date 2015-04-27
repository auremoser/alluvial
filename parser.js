var csv = require('csv-streamify'),
    fs = require('fs');

var writer = require('csv-write-stream')({
    separator:'|'
});
writer.pipe(fs.createWriteStream('./data/clean_students.csv'));


//if you need seconds make it TIME_UNIT = 1
var TIME_UNIT = 1000; //milliseconds,

var options= {
    delimiter: '|', // pipes!
    newline: '\n', // newline character
    quote: '"', // what's considered a quote
    // if true, emit arrays instead of stringified arrays or buffers
    objectMode: true,
    // if set to true, uses first row as keys -> [ { column1: value1, column2: value2 }, ...]
    columns: true
};

var fstream = fs.createReadStream('./data/student_data.csv'),
    parser = csv(options);

// emits each line as a buffer or as a string representing an array of fields
parser.on('readable', function () {
    var line = parser.read();
    // do stuff with data as it comes in
    line.id = parseInt(line.id);
    line.people = parsePeople(line.people);
    line.date = parseDate(line.date);
    line.time = parseTime(line.time);
    line.duration = parseDuration(line.duration);
    line.lat = parseFloat(line.lat);
    line.lon = parseFloat(line.lon);

    console.log(line);

    //We could do this, but since writer is also
    //a stream, we can pipe it!
    // writer.write(line);

}).on('end', function(){
    console.log('END');
    //We could do this, but since writer is also
    //a stream, we can pipe it!
    // writer.end();
});

fstream.pipe(parser).pipe(writer);


function parseDate(date){
    return (new Date(date).getTime() / 1000);
}

function parsePeople(people){
    people = (people || '').split(',');
    people = people.map(function(id){
        return parseInt(id);
    });
    return JSON.stringify(people);
}

function parseTime(time){
    time = time.split('\s')[0];
    var chunks = time.split(':');
    var hours = parseInt(chunks[0]);
    var minutes = parseInt(chunks[1]);
    var seconds = parseInt(chunks[2]);
    return ((hours * 60 * 60) + (minutes * 60) + seconds) * 1000;
}

function parseDuration(time){
    var chunks = time.split(':');
    var minutes = parseInt(chunks[0]);
    var seconds = parseInt(chunks[1]);
    return ((minutes * 60) + seconds) * 1000;
}