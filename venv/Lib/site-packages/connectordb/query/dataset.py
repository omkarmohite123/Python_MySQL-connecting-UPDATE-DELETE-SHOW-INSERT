from __future__ import absolute_import

from .._stream import Stream, query_maker
from .merge import Merge, get_stream
import six


# param_stream adds the stream correctly into the query (depending on what stream parameter was given)
def param_stream(cdb, params, stream):
    if isinstance(stream, Merge):
        params["merge"] = stream.query
    else:
        params["stream"] = get_stream(cdb, stream)


class Dataset(object):
    """ConnectorDB is capable of taking several separate unrelated streams, and based upon
    the chosen interpolation method, putting them all together to generate tabular data centered about
    either another stream's datapoints, or based upon time intervals.

    The underlying issue that Datasets solve is that in ConnectorDB, streams are inherently unrelated.
    In most data stores, such as standard relational (SQL) databases, and even excel spreadsheets, data is in tabular
    form. That is, if we have measurements of temperature in our house and our mood, we have a table:

        +--------------+----------------------+
        | Mood Rating  | Room Temperature (F) |
        +==============+======================+
        | 7            | 73                   |
        +--------------+----------------------+
        | 3            | 84                   |
        +--------------+----------------------+
        | 5            | 79                   |
        +--------------+----------------------+

    The benefit of having such a table is that it is easy to perform data analysis. You know which temperature
    value corresponds to which mood rating. The downside of having such tables
    is that Mood Rating and Room Temperature must be directly related - a temperature measurement must be made
    each time a mood rating is given. ConnectorDB has no such restrictions. Mood Rating and Room Temperature
    can be entirely separate sensors, which update data at their own rate. In ConnectorDB, each stream
    can be inserted with any timestamp, and without regard for any other streams.

    This separation of Streams makes data require some preprocessing and interpolation before it can be used
    for analysis. This is the purpose of the Dataset query. ConnectorDB can put several streams together based
    upon chosen transforms and interpolators, returning a tabular structure which can readily be used for ML
    and statistical applications.

    There are two types of dataset queries

    :T-Dataset:

        T-Dataset: A dataset query which is generated based upon a time range. That is, you choose a time range and a
        time difference between elements of the dataset, and that is used to generate your dataset.

            +--------------+----------------------+
            | Timestamp    | Room Temperature (F) |
            +==============+======================+
            | 1pm          | 73                   |
            +--------------+----------------------+
            | 4pm          | 84                   |
            +--------------+----------------------+
            | 8pm          | 79                   |
            +--------------+----------------------+

        If I were to generate a T-dataset from 12pm to 8pm with dt=2 hours, using the interpolator "closest",
        I would get the following result:

            +--------------+----------------------+
            | Timestamp    | Room Temperature (F) |
            +==============+======================+
            | 12pm         | 73                   |
            +--------------+----------------------+
            | 2pm          | 73                   |
            +--------------+----------------------+
            | 4pm          | 84                   |
            +--------------+----------------------+
            | 6pm          | 84                   |
            +--------------+----------------------+
            | 8pm          | 79                   |
            +--------------+----------------------+

        The "closest" interpolator happens to return the datapoint closest to the given timestamp. There are many
        interpolators to choose from (described later).

        Hint: T-Datasets can be useful for plotting data (such as daily or weekly averages).

    :X-Dataset:
        X-datasets allow to generate datasets based not on evenly spaced timestamps, but based upon a stream's values

        Suppose you have the following data:

            +-----------+--------------+---+-----------+----------------------+
            | Timestamp | Mood Rating  |   | Timestamp | Room Temperature (F) |
            +===========+==============+===+===========+======================+
            | 1pm       | 7            |   | 2pm       | 73                   |
            +-----------+--------------+---+-----------+----------------------+
            | 4pm       | 3            |   | 5pm       | 84                   |
            +-----------+--------------+---+-----------+----------------------+
            | 11pm      | 5            |   | 8pm       | 81                   |
            +-----------+--------------+---+-----------+----------------------+
            |           |              |   | 11pm      | 79                   |
            +-----------+--------------+---+-----------+----------------------+

        An X-dataset with X=Mood Rating, and the interpolator "closest" on Room Temperature would generate:

            +--------------+----------------------+
            | Mood Rating  | Room Temperature (F) |
            +==============+======================+
            | 7            | 73                   |
            +--------------+----------------------+
            | 3            | 84                   |
            +--------------+----------------------+
            | 5            | 79                   |
            +--------------+----------------------+

    :Interpolators:

        Interpolators are special functions which specify how exactly the data is supposed to be combined
        into a dataset. There are several interpolators, such as "before", "after", "closest" which work
        on any type of datapoint, and there are more advanced interpolators which require a certain datatype
        such as the "sum" or "average" interpolator (which require numerical type).

        In order to get detailed documentation on the exact interpolators that the version of ConnectorDB you are
        are connected to supports, you can do the following::

            cdb = connectordb.ConnectorDB(apikey)
            info = cdb.info()
            # Prints out all the supported interpolators and their associated documentation
            print info["interpolators"]

    """

    def __init__(self, cdb, x=None, t1=None, t2=None, dt=None, limit=None, i1=None, i2=None, transform=None, posttransform=None):
        """In order to begin dataset generation, you need to specify the reference time range or stream.

        To generate a T-dataset::
            d = Dataset(cdb, t1=start, t2=end, dt=tchange)
        To generate an X-dataset::
            d = Dataset(cdb,"mystream", i1=start, i2=end)

        Note that everywhere you insert a stream name, you are also free to insert Stream objects
        or even Merge queries. The Dataset query in ConnectorDB supports merges natively for each field.

        The only "special" field in this query is the "posttransform". This is a special transform to run on the
        entire row of data after the all of the interpolations complete.
        """
        self.cdb = cdb
        self.query = query_maker(t1, t2, limit, i1, i2, transform)

        if x is not None:
            if dt is not None:
                raise Exception(
                    "Can't do both T-dataset and X-dataset at the same time")
            # Add the stream to the query as the X-dataset
            param_stream(self.cdb, self.query, x)
        elif dt is not None:
            self.query["dt"] = dt
        else:
            raise Exception("Dataset must have either x or dt parameter")
        
        if posttransform is not None:
            self.query["posttransform"] = posttransform

        self.query["dataset"] = {}

    def addStream(self, stream, interpolator="closest", t1=None, t2=None, dt=None, limit=None, i1=None, i2=None, transform=None,colname=None):
        """Adds the given stream to the query construction. Additionally, you can choose the interpolator to use for this stream, as well as a special name
        for the column in the returned dataset. If no column name is given, the full stream path will be used.

        addStream also supports Merge queries. You can insert a merge query instead of a stream, but be sure to name the column::

            d = Dataset(cdb, t1=time.time()-1000,t2=time.time(),dt=10.)
            d.addStream("temperature","average")
            d.addStream("steps","sum")

            m = Merge(cdb)
            m.addStream("mystream")
            m.addStream("mystream2")
            d.addStream(m,colname="mycolumn")

            result = d.run()
        """

        streamquery = query_maker(t1, t2, limit, i1, i2, transform)
        param_stream(self.cdb, streamquery, stream)

        streamquery["interpolator"] = interpolator

        if colname is None:
            # What do we call this column?
            if isinstance(stream, six.string_types):
                colname = stream
            elif isinstance(stream, Stream):
                colname = stream.path
            else:
                raise Exception(
                    "Could not find a name for the column! use the 'colname' parameter.")

        if colname in self.query["dataset"] or colname is "x":
            raise Exception(
                "The column name either exists, or is labeled 'x'. Use the colname parameter to change the column name.")

        self.query["dataset"][colname] = streamquery

    def run(self):
        """Runs the dataset query, and returns the result"""
        return self.cdb.db.query("dataset", self.query)
