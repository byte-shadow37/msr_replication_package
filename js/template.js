
function menu(index, relativePath) {
	relativePath = typeof relativePath !== 'undefined' ? relativePath : '';
    var pages = ['index.html', 'publications.html', 'bibbase.html', 'students.html', 'projects.html', 'teaching.html', 'service.html', 'awards.html'];
    var titles = ['Home', 'Publications', 'BibBase', 'Students', 'Projects', 'Teaching', 'Service', 'Awards'];

    document.write('            <ul>');

    for (var i=0; i<pages.length; i++) {
        if (index == i) {
            document.write('				<li class="active"><a class="active" href="' + relativePath + pages[i] + '">' + titles[i] + '</a></li>');
        }
        else {
            document.write('				<li><a href="' + relativePath + pages[i] + '">' + titles[i] + '</a></li>');
        }
    }

    document.write('			</ul>');
}