Multi File Management / Directory Output
----------------------------------------

.. note::
    This is an experimental feature, which will probably be changed in future versions

Working with time sequences or images we often do not have the possibility to load all of our data
into one huge dataset into the memory of our local machine. Using :py:mod:`~luisy` can be a
challenge in this case, because having thousands of task instances (one task per image/curve) slows
down the scheduler, which results in long computation times.

This section shows you a template, that you can use when facing this problem. There are several
ways of handling multiple files, but we noticed that this was the most robust method.

Having lots of curves or images inside a directory requires us to use the :py:func:`luisy.decorators
.make_output_directory` function to create our own output decorator to read these files properly.
This allows us to define our own method of parsing/reading the files that are lying inside the
raw directory.

In the following pipeline we want to read all `'.png'` images one by one and process them in
other tasks. To do that, we need to define a function on how to handle the files that are
contained in external raw folder. Using the package :py:mod:`scikit-image`, we read each incoming
`'filepath'` with the method :py:func:`skimage.io.imread`. Note that only `'.png'` files are read
. The rest of the incoming files will be skipped by returning None in our function. In this case
we also want the filepath to be returned because the id of the curve is saved inside the file
basename.

The usage of :py:func:`~luisy.decorators.make_directory_output` allows us to pass our read
function to a decorator, which then decorates our :py:class:`~luisy.tasks.base.ExternalTask`.

.. code-block:: python

    from skimage import io
    import luisy
    from luisy.decorators import make_directory_output

    def read_image(filepath):
        if filepath.endswith('.png'):
            return filepath, io.imread(filepath)

    image_directory_output = make_directory_output(read_image)

    @image_directory_output
    @luisy.raw
    class ExportDirectory(luisy.ExternalTask):
        """
        Input directory with (multiple) image files
        """

        def get_folder_name(self):
            return ''

In the next step we want to require the files we have set in our :py:class:`ExportDirectory`
task above and save them as numpy arrays to a pickle file one by one.
:py:meth:`ExportDirectory().read` returns a generator, which yields the files to the time when
they're read. This gives us the advantage to work memory efficient. A challenge will now be to
continue to handle these files separately. This can be achieved by defining read/write
functionalities inside your task.

:py:mod:`luisy` usually works with :py:class:`luisy.targets.LocalTarget`. This class checks
whether the file it points to exists on your local machine. Since we have directories full
of files instead of individual files, we need to manage the files ourselves. A fairly clean way
of doing this is to process all files in a temporary directory and after processing, move them to
our target directory. We will then write all processed file_paths to a file in the same directory.

On the one hand, this prevents failed tasks from generating output (even when they fail), since
the files are not moved until all files have been successfully processed. On the other hand, this
makes further processing and subsequent tasks easier, since the information about the files is
saved in a list and can be read out in the next task.

.. code-block:: python

    import tempfile
    import pickle
    import shutil

    def write_file(file, filepath):
        pickle.dump(file, open(filepath, 'wb'))

    def read_file(filepath):
        return pickle.load(open(filepath, 'rb'))

    def get_image_id(filepath):
        return os.path.basename(filepath).split('.')[0]

    def move_file(source, destination):
        shutil(source, destination)
        return destination

    @luisy.interim
    @luisy.requires(ExportDirectory)
    # (@pickle_output) luisy.Task is pickle_output by default
    class ToNumpy(luisy.Task):
        def run(self):
            processed_files = []
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Process every file passed by the generator of required external Task
                for filepath, image in self.input().read():
                    image_id = get_image_id(filepath)
                    filename = image_id + '.pkl'
                    tmpfile = os.path.join(
                        tmp_dir,
                        filename,
                    )
                    write_file(image, tmpfile)
                    processed_files.append(filename)

                # Move successfully processed files to outdir
                saved_files = [move_file(
                    source=os.path.join(tmp_dir, file),
                    destination=os.path.join(self.get_outdir(), file)
                    ) for file in processed_files]

            # Tell luisy that task had success by writing the saved files to pickle
            self.write(saved_files)

Further tasks can use this pattern on and on until you preprocessed your data well enough for
training. Let's for example take the images and filter on the red channel of the image data:

.. code-block:: python

    @luisy.final
    @luisy.requires(ToNumpy)
    class RedChannel(luisy.Task):
        def get_image_id(self, filename):
            return os.path.basename(filepath)

        def read_files(self):
            for filepath in self.input().read():
                image_id = get_image_id(filepath)
                data = read_file(filepath)
                yield image_id, data

        def run():
            processed_files.append(tmpfile)
            with tempfile.TemporaryDirectory() as tmp_dir:
                for image_id, data in self.read_files():
                    red_channel = data[:,:,0]
                    filename = image_id + '.pkl'
                    tmp_file = os.path.join(
                        tmp_dir,
                        filename
                    )
                    write_file(red_channel, tmp_file)
                    processed_files.append(filename)

                # Move successfully processed files to outdir
                saved_files = [self.move_file(
                    source=os.path.join(tmp_dir, file),
                    destination=os.path.join(self.get_outdir(), file)
                    ) for file in processed_files]
            self.write(saved_files)

This way of dealing with multiple files comes with a little bit of boilerplate inside your
pipeline, but seems like the most robust way. Future updates of luisy will probably improve this
functionality, so stay tuned.
