# Concatentor code

# DataSetEntry for keeping track of file and datasets that we are concatenating
mutable struct DataSetEntry
    name::String
    type::Type
    size::Int64
    filePlaces::Dict

    DataSetEntry(anObject) = new(HDF5.name(anObject), eltype(anObject), 0, Dict())
end

""" addSize(e, fileName, size)

    Adjust the DataSetEntry for this dataset and this input file of size.
    Determine where data from this file will go in the output file.
"""
function addSize!(e::DataSetEntry, fileName::String, size)
    startAt = e.size + 1
    endAt   = startAt + size - 1
    e.filePlaces[fileName] = (startAt, endAt)
    e.size += size
end

""" visitH5Contents(fileName, inH5, visitor)

    Walk the contents of an HDF5 file, visiting each group and dataset in the hierarchy.

    fileName is the name of the file we're traversing. inH5 is the file object or group object
    to walk.

    This functions will walk within HDF5 file and group objects and will
    recursively dive into a hierarchy of groups. The visted object is passed
    to the visitor function (it must handle whatever object is passed in).
"""
function visitH5Contents(fileName::String, inH5::Union{HDF5File, HDF5Group}, visitor)
    for anObject in inH5
        visitor(anObject, fileName)           # Process this object
        if typeof(anObject) == HDF5Group   # If this object is a group then walk inside
            visitH5Contents(fileName, anObject, visitor)
        end
    end

"""makeGetStructureVisitor(groups, datasets)

    This is a function to be used as the visitor by visitH5Contents. It
"""
function makeGetStructureVisitor(groups, datasets)
    function theVisitor(anObject, fileName)
        objectName = HDF5.name(anObject)
        #@debug "$myrank visiting $objectName"
        if typeof(anObject) == HDF5Group  # If group, keep track
            if ! (objectName in groups)
               push!(groups, objectName)
            end
        elseif typeof(anObject) == HDF5Dataset  # If dataset, add up length
            if ! (haskey(datasets, objectName))
                datasets[objectName] = DataSetEntry(anObject)
            end
            addSize!(datasets[objectName], fileName, length(anObject))
        else
            @error "Unknown HDF5 type of $(typeof(anObject))"
        end
    end
end

""" inputFilesStructure(fileList)

    Scan the input files and examine their structure. Return
    a list of the groups found in the input files as well as
    a dictionary of DataSetEntry objects. The dictionary is keyed on
    the full name of the dataset. Each value is the DataSetEntry object
    which contains the type of the dataset, the total number of entries
    in the dataset (e.g. number of rows for 1D, total number of entries for
    multidimensional).

    Note that right now we assume that input datasets are of dimension (1,N) and the output is meant to be dimension N.

    The dataSetEntry also contains a dictionary. Each entry is keyed
    off of the input file name. The value is a tuple of `(startAt, endAt)`
    which specifies the indices of an output file where this file's data should go.
"""
function inputFilesStructure(fileList)
    groups = Vector{String}()
    dataSets = Dict()

    getStructureVisitor = makeGetStructureVisitor(groups, dataSets)

    # This happens serially, but it's quite fast
    for fileName in fileList
        h5open(fileName, "r") do inH5
            visitH5Contents(fileName, inH5, getStructureVisitor)
        end
    end

    return groups, dataSets
end
