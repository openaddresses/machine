import os
import errno
import csv

from logging import getLogger
from zipfile import ZipFile

from osgeo import ogr, osr
ogr.UseExceptions()


def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class ConformResult:
    processed = None
    sample = None
    path = None
    elapsed = None
    
    # needed by openaddr.process.write_state(), for now.
    output = ''

    def __init__(self, processed, sample, path, elapsed):
        self.processed = processed
        self.sample = sample
        self.path = path
        self.elapsed = elapsed

    @staticmethod
    def empty():
        return ConformResult(None, None, None, None)

    def todict(self):
        return dict(processed=self.processed, sample=self.sample)


class DecompressionError(Exception):
    pass


class DecompressionTask(object):
    @classmethod
    def from_type_string(clz, type_string):
        if type_string == None:
            return NoopDecompressTask()
        elif type_string.lower() == 'zip':
            return ZipDecompressTask()
        else:
            raise KeyError("I don't know how to decompress for type {}".format(type_string))

    def decompress(self, source_paths):
        raise NotImplementedError()


class NoopDecompressTask(DecompressionTask):
    def decompress(self, source_paths, workdir):
        return source_paths


class ZipDecompressTask(DecompressionTask):

    logger = getLogger().getChild('unzip')

    def decompress(self, source_paths, workdir):
        output_files = []
        expand_path = os.path.join(workdir, 'unzipped')
        mkdirsp(expand_path)

        for source_path in source_paths:
            with ZipFile(source_path, 'r') as z:
                for name in z.namelist():
                    expanded_file_path = z.extract(name, expand_path)
                    self.logger.debug("Expanded file %s", expanded_file_path)
                    output_files.append(expanded_file_path)
        return output_files

class ExcerptDataTask(object):
    ''' Task for sampling three rows of data from datasource.
    '''
    logger = getLogger().getChild('excerpt')
    known_types = ('.shp', '.json', '.csv', '.kml')

    def excerpt(self, source_paths, workdir):
        '''
        '''
        known_paths = [source_path for source_path in source_paths
                       if os.path.splitext(source_path)[1] in self.known_types]
        
        if not known_paths:
            # we know nothing.
            return None
        
        datasource = ogr.Open(known_paths[0], 0)
        layer = datasource.GetLayer()

        layer_defn = layer.GetLayerDefn()
        fieldnames = [layer_defn.GetFieldDefn(i).GetName()
                      for i in range(layer_defn.GetFieldCount())]

        data_sample = [fieldnames]
        
        for feature in layer:
            data_sample.append([feature.GetField(i) for i
                                in range(layer_defn.GetFieldCount())])

            if len(data_sample) == 4:
                break
        
        return data_sample

class ConvertToCsvTask(object):

    logger = getLogger().getChild('convert')

    known_types = ('.shp', '.json', '.csv', '.kml')

    def convert(self, source_paths, workdir):

        output_files = []
        convert_path = os.path.join(workdir, 'converted')
        mkdirsp(convert_path)

        for source_path in source_paths:
            filename = os.path.basename(source_path)
            basename, ext = os.path.splitext(filename)
            file_path = os.path.join(convert_path, basename + '.csv')


            if ext not in self.known_types:
                self.logger.debug("Skipping %s because I don't know how to convert it", source_path)
                continue
            if os.path.exists(file_path):
                output_files.append(file_path)
                self.logger.debug("File exists %s", file_path)
                continue

            in_datasource = ogr.Open(source_path, 0)
            in_layer = in_datasource.GetLayer()
            inSpatialRef = in_layer.GetSpatialRef()

            self.logger.info("Converting a layer to CSV: %s", in_layer)

            in_layer_defn = in_layer.GetLayerDefn()
            out_fieldnames = []
            for i in range(0, in_layer_defn.GetFieldCount()):
                field_defn = in_layer_defn.GetFieldDefn(i)
                out_fieldnames.append(field_defn.GetName())
            out_fieldnames.append('centroid')

            outSpatialRef = osr.SpatialReference()
            outSpatialRef.ImportFromEPSG(4326)
            coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

            with open(file_path, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=out_fieldnames)
                writer.writeheader()

                in_feature = in_layer.GetNextFeature()
                while in_feature:
                    row = dict()

                    for i in range(0, in_layer_defn.GetFieldCount()):
                        field_defn = in_layer_defn.GetFieldDefn(i)
                        row[field_defn.GetNameRef()] = in_feature.GetField(i)
                    geom = in_feature.GetGeometryRef()
                    geom.Transform(coordTransform)
                    row['centroid'] = geom.Centroid().ExportToWkt()

                    writer.writerow(row)

                    in_feature.Destroy()
                    in_feature = in_layer.GetNextFeature()

            in_datasource.Destroy()
            output_files.append(file_path)

        return output_files