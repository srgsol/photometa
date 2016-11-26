# -*- coding: utf8 -*-
"""
Photo Metadata

- Import album
- Describe source files paths (ha de permetre descobrir albums)

EXIF info:
- http://stackoverflow.com/questions/4764932/in-python-how-do-i-read-the-exif-data-for-an-image
- https://www.quora.com/What-is-the-difference-between-Date-and-Time-Original-and-Date-and-Time-in-the-EXIF-data-output

    from PIL import Image
    im1 = Image.open('/home/sergi/Pictures/2010/01/23/img_0004.jpg')
    exif_data = im1._getexif()
    exif_data

    from PIL import Image
    from PIL import ExifTags
    im1 = Image.open('/home/sergi/Pictures/2010/01/23/img_0004.jpg')
    exif = {
        ExifTags.TAGS[k]: v
        for k, v in self.__img._getexif().items()
        if k in ExifTags.TAGS
    }

"""
import os
import re
import collections
from PIL import Image
from PIL import ImageChops
from PIL import ExifTags
import hashlib
from datetime import datetime
import shutil
import struct
import logging

from logging_conf import logger_factory


logger_factory('logger_err')
logger_err = logging.getLogger('logger_err')

logger_factory('logger_trans')
logger_trans = logging.getLogger('logger_trans')


# TODO. logging.exception(). Handler to file and stdout.
# TODO. Alternatively to logging.
# Look at traceback module: traceback.print_exc(file = sys.stdout)

# TODO. Use non ambiguous terminlolgy.
# Import, copy, insert are being used without distinction

BASE_PATH = "/home/sergi/Pictures"

EXIF_DATE_CREATE_CODE = 306
EXIF_DATE_ORIGINAL_CODE = 36867

# Permission to insert files in the repository.
REPO_IS_LOCKED = True

# Permission to allow overwrite files when importing
ALLOW_OVERWRITE = False


class PhotoException(Exception):
    pass


class ImporterException(Exception):
    pass


class RepositoryManager(object):
    """Manages the Repository and its operations

    Given a Repository, a SourceFilesManager, and an optional DataBase,
    coordinates all interactions between these entities.
    """
    def __init__(self, repository, source_fm):
        self.__repo = repository
        self.__source_fm = source_fm

        # SourceFiles with import error.
        self.__sf_with_import_error = []

    def __insert(self, overwrite=False, alternate_names=False, dry_run=False):

        for source_file in self.__source_fm.files:
            # TODO. Que passa si hi ha un error al insert, per mès que previament
            # haguem fet un sfm.check??
            try:
                # import pdb; pdb.set_trace()
                self.__repo.insert(
                    source_file, dest_path=None, overwrite=overwrite,
                    alternate_names=alternate_names, dry_run=dry_run)
                logger_trans.info('Insert OK {}'.format(source_file))
            except Exception, ex:
                logger_trans.error('Insert ERROR {}'.format(source_file))
                logger_err.exception('Insert exception')
                self.__sf_with_import_error.append(source_file)

    def insert_strict(self, dry_run=False):
        """Raise error if file exist in the destination path.

        If there exist a file with the same name as the source file being
        imported, raise error.

        :param dry_run: bool
        :return:
        """
        self.__insert(overwrite=False, alternate_names=False, dry_run=dry_run)


class Repository(object):
    """Photo Repository

    TODO
        - Load repo settings from file (path, insert path policy, overwrite, etc.
        - Logging
    """
    def __init__(self, path=None):
        # It can be None in case of new repository.
        self.__path = path
        self.__sfm = SourceFilesManger(path)

        if self.__path is not None:
            try:
                _check_path(self.__path)
            except ValueError, ex:
                raise ValueError('Error in Repository: {}'.format(ex.message))

    @property
    def path(self):
        return self.__path

    def create(self, path):
        """Create a new repository.

        Create a new directory at the given path. Create all necessary
        directories in the path if necessary.
        """
        if self.__path is None:
            os.makedirs(path)
        self.__path = path

    def is_valid(self):
        """Check if the repository has a valid path."""
        # At object construction time the path has been checked to ensure is
        # a valid path.
        return self.__path is not None

    def insert(self, source_file, dest_path=None,
               overwrite=False, alternate_names=False, dry_run=False):
        """Create an importer object to insert the source file in the repository.

        It can be seen as a factory method which instantiate a concrete insert
        strategy.
        """
        if not self.is_valid():
            raise ValueError('Repository is not valid. Create a new one or '
                             'instantiate it at a valid path.')

        if overwrite and alternate_names:
            raise ValueError(
                "It's not possible to set overwrite=True and alternate_names=True. "
                "alternate_names=True set an alternate name in case of duplicate "
                "file, so we can't overwrite it, because we are creating a new "
                "file.")

        elif overwrite and not alternate_names:
            # In case of duplicate file name, overwrite file.
            importer_class = RepositoryImporterOverwrite

        elif not overwrite and alternate_names:
            # In case of duplicate file name, import with an alternate name.
            importer_class = RepositoryImporterAlternateName

        elif not overwrite and not alternate_names:
            # In case of duplicate file name stop and raise error.
            importer_class = RepositoryImporterStrict

        else:
            importer_class = None

        dest_path_callback =self.__dest_path_factory(source_file, dest_path)

        # try:
        #     repo_importer = importer_class(
        #         source_file, dest_path_callback, dry_run)
        #     repo_importer.insert()
        # except Exception, ex:
        #     # TODO Logging
        #     #traceback.print_exc(file=sys.stdout)
        #     raise ex

        repo_importer = importer_class(
            source_file, dest_path_callback, dry_run)
        repo_importer.insert()

    def __dest_path_factory(self, source_file, dest_path):
        if dest_path is None:
            return DestPathYearMonth(self, source_file)
        elif isinstance(dest_path, str):
            return DestPathFixed(self, dest_path)

    def check(self):
        self.__sfm.check()

    def describe(self):
        return self.__sfm.describe()

    def describe_paths(self):
        return self.__sfm.describe_paths()

    def scan(self, db):
        """Scan through all directories in the repo to build the database."""
        raise NotImplementedError()

    def __repr__(self):
        if self.__path is not None:
            return "Repository('{}')".format(self.__path)
        else:
            return "Repository(None)"


class SourceFilesManger(object):
    """Manages a set of SourceFiles.

    factory = {
        'jpg': (SourceFileDateFromName,
    }
    """
    def __init__(self, path, recursive=True, to_lower=False, regexp=None,
                 exclude_ext=None, factory=None):
        self.__path = path
        self.__recursive = recursive
        self.__to_lower = to_lower
        self.__regexp = regexp
        self.__exclude_ext = exclude_ext
        self.__factory = factory

        # Path to all files in the source.
        self.__spaths = None

        # Concrete SourceFiles objects for all files in the source.
        self.__sfiles = None

        # Check the given path is ok.
        try:
            _check_path(self.__path)
        except ValueError, ex:
            raise ValueError('Error in SourceFilesManager: {}'.format(ex.message))

        # Load Source Files
        self.__load()

    @property
    def files(self):
        return self.__sfiles

    def files_with_date_error(self):
        # self.__load()
        # TODO return generator
        return [sf for sf in self.__sfiles
                if sf.has_date_error]

    def __factory_method(self, sf):
        if sf.extension.lower() in ['jpg']:
            return SourceFileEXIF(sf.fpath)
        elif sf.extension.lower() in ['mov', 'mp4']:
            return SourceFileMPEG4(sf.fpath)
        else:
            # Generic SourceFile
            return sf

    def __load(self):
        if self.__sfiles is None:
            # TODO implementar generador
            # http://stackoverflow.com/questions/19151/build-a-basic-python-iterator
            self.__sfiles = [self.__factory_method(SourceFile(sp))
                             for sp in self.source_paths]

    def __read_source_paths(self):
        """Read and return the path for all files in the SourceFileManager path."""
        self.__spaths = files_in_folder(
            self.__path, recursive=self.__recursive, to_lower=self.__to_lower,
            regexp=self.__regexp, exclude_ext=self.__exclude_ext)
        return self.__spaths

    @property
    def source_paths(self):
        """Return the path for all files in the SourceFileManager path."""
        if self.__spaths is None:
            self.__read_source_paths()
            self.__read_source_paths()
        return self.__spaths

    def describe(self):
        """Print type and number of files in the source."""
        # self.__load()
        extensions = [f.extension for f in self.__sfiles]
        counter = collections.Counter(extensions)
        return list(counter.iteritems())

    def describe_paths(self):
        return list(collections.Counter([sf.path for sf in self.files]).iteritems())

    def check(self):
        total_proc= 0
        total_err = 0
        total = len(self.__sfiles)

        for sf in self.__sfiles:
            total_proc += 1
            if sf.has_date_error:
                total_err += 1
                print sf.date_error_message

        print 'Total files: {}'.format(total)
        print 'Total processed files: {}'.format(total_proc)
        print 'Error files: {}'.format(total_err)

    def __len__(self):
        # self.__load()
        return len(self.__sfiles)

    def __repr__(self):
        return "SourceFilesManger('{}')".format(self.__path)


class SourceFile(object):
    """File to be included in the repository."""
    def __init__(self, fpath):
        self._fpath = fpath
        self._date_create = None
        # self.__has_import_error = False
        self.__has_date_error = False
        self.__date_error_message = None

        if not os.path.exists(fpath):
            raise ValueError(
                "Given path doesn't exist: {}".format(fpath))
        if not os.path.isfile(fpath):
            raise ValueError(
                "Given path is not a file: {}".format(fpath))

        self.__check_date_create()

    @property
    def fpath(self):
        """Source file path (path + filename). Path from where we want to read."""
        return self._fpath

    @property
    def path(self):
        """Return the file path without filename."""
        return os.path.dirname(self._fpath)

    @property
    def basename(self):
        """File basename.

        Given the file path, basename is the trailing part after the last slash.
        """
        return os.path.basename(self._fpath)

    @property
    def name(self):
        """File name without extension."""
        return os.path.splitext(os.path.basename(self._fpath))[0]

    @property
    def extension(self):
        """File extension."""
        return os.path.splitext(self._fpath)[1][1:]

    def hash(self):
        """Compute a MD5 hash."""
        with open(self._fpath, 'rb') as f:
            hasher = hashlib.md5()
            hasher.update(f.read())
            hsh = hasher.hexdigest()
            return hsh

    def date_create(self):
        raise NotImplementedError("Subclasses must implement 'date_create' method.")

    @property
    def has_date_error(self):
        return self.__has_date_error

    @property
    def date_error_message(self):
        return self.__date_error_message

    def __check_date_create(self):
        self.__has_date_error = True

        try:
            self.date_create()
            self.__has_date_error = False

        except NotImplementedError:
            self.__date_error_message = (
                "{} Unexpected file type '{}'. You can exclude the "
                "extension.".format(self.fpath, self.extension))
        except PhotoException, ex:
            self.__date_error_message = ex.message
        except Exception, ex:
            self.__date_error_message = (
                "{} {} Unexpected "
                "error: {}.".format(self.fpath, self.__name__, ex.message))

    def __repr__(self):
        return "{0}('{1}')".format(self.__class__.__name__, self._fpath)


class SourceFileEXIF(SourceFile):
    """EXIF file. This includes .jpg"""
    def __init__(self, *args, **kwargs):
        super(SourceFileEXIF, self).__init__(*args, **kwargs)
        self.__img = None

    def __load(self):
        try:
            self.__img = Image.open(self._fpath)
            # self.__img = Image.open(open(self._fpath, 'rb'))
        except IOError, ex:
            msg = ex.message
            if ex.message == '':
                try:
                    # Some IOErrors are tuple (code, message),
                    # like IOError (24, 'Too many open files')
                    msg = ex[1]
                except:
                    msg = ''
            raise PhotoException("{} SourceFileEXIF IOError: '{}'".format(self._fpath, msg))

    def __exif_data(self):
        try:
            data = self.__img._getexif()
            if not data:
                raise PhotoException('{} SourceFileEXIF Empty exif data.'.format(self._fpath))
            return data
        except:
            raise PhotoException('{} SourceFileEXIF Missing EXIF data'.format(self._fpath))
        finally:
            self.__img.close()

    def date_create(self):
        if self._date_create is not None:
            return self._date_create
        self.__load()
        exif_data = self.__exif_data()
        try:
            create = exif_data[EXIF_DATE_ORIGINAL_CODE][0]
        except KeyError:
            raise PhotoException('{} SourceFileEXIF EXIF data does not have '
                           'creation date.'.format(self._fpath))
        try:
            return datetime.strptime(create, '%Y:%m:%d %H:%M:%S')
        except ValueError, ex:
            raise PhotoException('{} SourceFileEXIF invalid EXIF '
                                 'data: {}.'.format(self._fpath, ex.message))


    @property
    def exif_data(self, tags=True):
        self.__load()
        exif_data = self.__exif_data()
        if tags:
            exif = {
                ExifTags.TAGS[k]: v
                for k, v in exif_data.items()
                if k in ExifTags.TAGS
            }
            return exif
        else:
            return exif_data


class SourceFileMPEG4(SourceFile):
    """MPEG-4 file. This includes .mov and .mp4 files

    see: ttps://en.wikipedia.org/wiki/QuickTime_File_Format
    """
    def date_create(self):
        if self._date_create is None:
            try:
                self._date_create = self.__mpeg4_creation_date(self._fpath)
            except struct.error, ex:
                raise PhotoException("{} SourceFileMPEG4: struct.error: '{}'".format(
                    self._fpath, ex.message))
            except ValueError, ex:
                raise PhotoException("{} SourceFileMPEG4 struct.error: '{}'".format(
                    self._fpath, ex.message))
        return self._date_create

    def __mpeg4_creation_date(self, fpath):
        """Read QuickTime MOV and MP4 files creation date.

        http://stackoverflow.com/questions/21355316/getting-metadata-for-mov-video
        """
        import struct

        ATOM_HEADER_SIZE = 8
        EPOCH_ADJUSTER = 2082844800

        f = None

        # open file and search for moov item

        f = open(fpath, 'rb')
        while 1:
            atom_header = f.read(ATOM_HEADER_SIZE)
            if atom_header[4:8] == 'moov':
                break
            else:
                atom_size = struct.unpack(">I", atom_header[0:4])[0]
                f.seek(atom_size - 8, 1)

        # found 'moov', look for 'mvhd' and timestamps
        atom_header = f.read(ATOM_HEADER_SIZE)
        if atom_header[4:8] == 'cmov':
            raise PhotoException("{} SourceFileMPEG4: MPEG-4 err. 'moov' atom "
                                 "is compressed.".format(self._fpath))
        elif atom_header[4:8] != 'mvhd':
            raise PhotoException("{} SourceFileMPEG4: MPEG-4 errMPEG-4. Expected "
                                 "to find 'mvhd' header.".format(self._fpath))
        else:
            f.seek(4, 1)

            creation_date = struct.unpack(">I", f.read(4))[0]
            creation_date = datetime.utcfromtimestamp(creation_date - EPOCH_ADJUSTER)

            # modification_date = struct.unpack(">I", f.read(4))[0]
            #modification_date = datetime.utcfromtimestamp(
            #    modification_date - EPOCH_ADJUSTER)

            return creation_date


class SourceFileDateFromName(SourceFile):
    """Source File whose creation date can be extracted from its file name.

    Example:
        2016-08-23 14.23.15.jpg
        This is the case for Dropbox Camera Upload files.
    """
    def __init__(self, fpath, regex=None, format=None):
        super(SourceFileDateFromName, self).__init__(fpath)
        self.__regex = regex
        self.__format = format

    def date_create(self):
        if self.__regex is None:
            # Dropbox Camera Upload format
            match = re.search('\d{4}-\d{2}-\d{2}\s\d{2}\.\d{2}\.\d{2}', self.name)
            if match:
                date_string = match.group()
        else:
            match = re.search(self.__regex, self.name)
            if match:
                date_string = match.group()

        if self.__format is None:
            # Dropbox Camera Upload format
            datetime_format = '%Y-%m-%d %H.%M.%S'
        else:
            datetime_format = self.__format

        try:
            date = datetime.strptime(date_string, datetime_format)
        except Exception:
            raise PhotoException("{} SourceFileDateFromName: Can't extract date "
                                 "from file name is compressed.".format(self._fpath))
        return date


class AbstractRepositoryImporter(object):
    """
    """
    def __init__(self, source_file, dest_path_callback, dry_run):
        self._source_file = source_file
        self.__dest_path_callback = dest_path_callback
        self.__dry_run = dry_run

        self.__alternate_name_sufix = 0

    def dest_path(self):

        # return os.path.join(
        #     self.__repo_path,
        #     self.__dest_path_callback(self._source_file.date_create())
        # )
        return self.__dest_path_callback()

    def insert(self):
        raise NotImplementedError()

    def _copy(self, dest_fname):

        # Check if destination filename collides with a directory name.
        dest_fpath = os.path.join(self.dest_path(), dest_fname)
        if os.path.isdir(dest_fpath):
            raise ValueError('Error: Destination file is an existing directory:{}'.
                             format(dest_fpath))

        if self.__dry_run:
            self.__copy_dry_run(dest_fname)
        else:
            self.__copy_to_disk(dest_fname)

    def __copy_dry_run(self, dest_fname):
        dest_fpath = os.path.join(self.dest_path(), dest_fname)
        source_fpath = self._source_file.fpath

        print 'DRY COPY', source_fpath, 'TO', dest_fpath

    def __copy_to_disk(self, dest_fname):
        # Note: In the following code there's a race condition:  if the
        # directory is created between the os.path.exists and the os.makedirs
        # calls, the os.makedirs will fail with an OSError.
        #
        # if not os.path.exists(dst):
        #     os.makedirs(dst)
        #
        # The following code solves the race condition
        try:
            # Try to build the path.
            os.makedirs(self.dest_path())
        except OSError:
            # If directory path exist, then OSError is raised, but other OSError
            # may arise (file permissions, etc). Check if the error is due to the
            # fact the directory exist. If it is not, then raise de OSError,
            # which may be file permission error or what ever.
            if not os.path.isdir(self.dest_path()):
                raise

        dest_fpath = os.path.join(self.dest_path(), dest_fname)
        source_fpath = self._source_file.fpath

        # Check overwrite permission
        if not ALLOW_OVERWRITE:
            # Overwrite is not allowed. Check if file exists.
            if os.path.isfile(os.path.join(self.dest_path(), dest_fname)):
                # File exist. Raise error!
                raise ValueError('File exsit: {}'.format(self.dest_path()))

        if not REPO_IS_LOCKED:
            # copy2: copy file and attributes
            shutil.copy2(source_fpath, dest_fpath)

            print 'COPY', source_fpath, 'TO', dest_fpath
        else:
            raise ValueError('Repository is locked!')


class RepositoryImporterAlternateName(AbstractRepositoryImporter):
    """
    """
    def __alternative_filename(self):
        """Build alternate filename.

            filename.jpg --> filename_1.jpg
        """
        fname, fext = os.path.splitext(self._source_file.basename)
        self.__alternate_name_sufix += 1
        fname = fname + "_" + str(self.__alternate_name_sufix)
        fext = fext[1:]
        alternate_name = fname + "." + fext
        return alternate_name

    def insert(self):
        """
        - Destination path is composed by repository path, which may exist or not,
        and source_file path which may exist or not.
        - Complete destination path (path + filename) may exist as a directory.
        - Complete destination path (path + filename) may exist as a file.
        """
        # Check if destination file exist.
        dest_fname = self._source_file.basename
        while os.path.isfile(os.path.join(self.dest_path(), dest_fname)):
            # File exist. Build an alternate name.
            dest_fname = self.__alternative_filename()

        # # Check if destination filename collides with a directory name.
        # dest_fpath = os.path.join(self.dest_path(), dest_fname)
        # if os.path.isdir(dest_fpath):
        #     raise ValueError('Error: Destination file is an existing directory:{}'.
        #                      format(dest_fpath))

        self._copy(dest_fname)


class RepositoryImporterOverwrite(AbstractRepositoryImporter):
    """
    """
    def insert(self):
        """
        """
        self._copy(self._source_file.basename)


class RepositoryImporterStrict(AbstractRepositoryImporter):
    """
    """
    def insert(self):
        """
        """
        fpath_lower = os.path.join(
            self.dest_path(), self._source_file.basename.lower())

        fpath_upper = os.path.join(
            self.dest_path(), self._source_file.basename.upper())

        # Check if file exists.
        for fpath in [fpath_lower, fpath_upper]:
            if os.path.isfile(fpath):
                raise ImporterException('{}. File exsit: {}'.format(
                    self.__class__.__name__, fpath))

        self._copy(self._source_file.basename)


class DestPath(object):
    def __init__(self, repo, source_file):
        self._repo = repo
        self._sf = source_file


class DestPathYearMonth(DestPath):
    def __call__(self):
        repo_path = self._repo.path
        date_create = self._sf.date_create()

        return os.path.join(
            repo_path,
            date_create.strftime("%Y/%m")
        )


class DestPathFixed(DestPath):
    def __init__(self, repo, dest_path):
        super(DestPathFixed, self).__init__(repo, None)
        self.__dest_path = dest_path

    def __call__(self):
        repo_path = self._repo.path
        return os.path.join(
            repo_path,
            self.__dest_path
        )


def equal(im1, im2):
    """

    im1 = Image.open('/home/sergi/Pictures/2010/01/23/img_0004.jpg')
    im2 = Image.open('/home/sergi/Pictures/2010/01/23/img_0005.jpg')
    equal(im1, im2)

    See also the method hash().
    """
    return ImageChops.difference(im1, im2).getbbox() is None


def hash(im1, im2):
    """
    im1 = '/home/sergi/Pictures/2010/01/23/img_0004.jpg'
    im2 = '/home/sergi/Pictures/2010/01/23/img_0005.jpg'
    has(im1, im2)

    This is x30 faster than the method equals().
    """
    with open(im1, 'r') as f1:
        hasher = hashlib.md5()
        hasher.update(f1.read())
        hsh1 = hasher.hexdigest()
    with open(im2, 'r') as f2:
        hasher = hashlib.md5()
        hasher.update(f2.read())
        hsh2 = hasher.hexdigest()
    return hsh1 == hsh2


def _check_path(path):
    if path is None:
        raise ValueError('Path cannot be None.')
    if not os.path.exists(path):
        raise ValueError(
            "Given source path doesn't exist: {}".format(path))
    if not os.path.isdir(path):
        raise ValueError(
            "Given source is not a directory: {}".format(path))


def files_in_folder(path, recursive=True, to_lower=False, regexp=None,
                    exclude_ext=None):
    """
    to_lower is applied only to file names, not to path string.

    to_lower is applied before than regexp does, so regexp has to take in to 
    account that it must be prepared for text which has been transformed to lower.
    """
    files = []
    if exclude_ext is not None:
        exclude_ext = [ext.lower() for ext in exclude_ext]
    else:
        exclude_ext = []
    for (dir_path, dir_names, file_names) in os.walk(path):

        for fname in file_names:

            if to_lower:
                fname = fname.lower()

            ext = os.path.splitext(fname)[1][1:]
            if ext in exclude_ext:
                continue

            fpath = os.path.join(dir_path, fname)

            if regexp is not None:
                if re.search(regexp, fpath):
                    files.append(fpath)
            else:
                files.append(fpath)

        if not recursive:
            break

    return files


def find_duplicates(l1, l2):
    # Intersection is commutative

    if duplicates_in_list(l1):
        raise ValueError("There are duplicates in l1.")

    if duplicates_in_list(l2):
        raise ValueError("There are duplicates in l2.")

    # fnames1 = [os.path.basename(fpath) for fpath in l1]
    # fnames2 = [os.path.basename(fpath) for fpath in l2]

    l1.extend(l2)
    return duplicates_in_list(l1)


def duplicates_in_list(lst):
    """Given a list of paths, find duplicates in file names.
        [
            '/home/sergi/Pictures/2013/09/09/img_0950.jpg',
            '/home/sergi/Pictures/2011/05/20/img_0950.jpg',
        ]
    """
    fnames = [os.path.basename(fpath) for fpath in lst]
    dup_fnames = [x for x, y in collections.Counter(fnames).items() if y > 1]
    d = [fpath
        for x in dup_fnames
        for fpath in lst
        if os.path.basename(fpath)==x]
    return d


def insert(path):
    sfm = SourceFilesManger(path)
    repo = Repository('/home/sergi/Dropbox/python/app/photometa/respo_test')
    rm = RepositoryManager(repo, sfm)
    rm.insert()
    return sfm

def check_input(path):
    sfm = SourceFilesManger(path, exclude_ext=['png', 'gif'])
    sfm.check()
    return sfm

if __name__ == '__main__':
    equal()