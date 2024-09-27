%% -------- Metadata Seperation - Head and Body ---------
%%
%% The definition of the part of the object that belongs to the HEAD, and
%% the part which belongs to the body.
%%
%% For the ?RIAK tag this is pre-defined.  For the ?STD_TAG there is minimal
%% definition.  For best use of Riak define a new tag and use pattern matching
%% to extend these exported functions.
%%
%% Dynamic user-defined tags are allowed, and to support these user-defined
%% shadow versions of the functions:
%% - key_to_canonicalbinary/1 -> binary(),
%% - build_head/2 -> head(),
%% - extract_metadata/3 -> {std_metadata(), list(erlang:timestamp()}
%% That support all the user-defined tags that are to be used

-module(leveled_head).

-include("leveled.hrl").

-export([key_to_canonicalbinary/1,
            build_head/2,
            extract_metadata/3,
            diff_indexspecs/3
            ]).

-export([get_size/2,
            get_hash/2,
            defined_objecttags/0,
            default_reload_strategy/1,
            standard_hash/1
            ]).

%% Exported for testing purposes
-export(
    [
        riak_metadata_to_binary/2,
        riak_extract_metadata/2,
        get_indexes_from_siblingmetabin/2
    ]).


-define(MAGIC, 53). % riak_kv -> riak_object
-define(V1_VERS, 1).

-type object_tag() :: ?STD_TAG|?RIAK_TAG.
    % tags assigned to objects
    % (not other special entities such as ?HEAD or ?IDX)
-type headonly_tag() :: ?HEAD_TAG.
    % Tag assigned to head_only objects.  Behaviour cannot be changed

-type riak_metadata() :: {binary()|delete, 
                                % Sibling Metadata
                            binary()|null, 
                                % Vclock Metadata
                            non_neg_integer()|null, 
                                % Hash of vclock - non-exportable 
                            non_neg_integer()
                                % Size in bytes of real object
                            }. 
-type std_metadata() :: {non_neg_integer()|null, 
                                % Hash of value 
                            non_neg_integer(), 
                                % Size in bytes of real object
                            list(tuple())|undefined
                                % User-define metadata
                            }.
-type head_metadata() :: {non_neg_integer()|null, 
                                % Hash of value 
                            non_neg_integer()
                                % Size in bytes of real object
                            }.

-type object_metadata() :: riak_metadata()|std_metadata()|head_metadata().

-type appdefinable_function() ::
    key_to_canonicalbinary | build_head | extract_metadata | diff_indexspecs.
        % Functions for which default behaviour can be over-written for the
        % application's own tags
-type appdefinable_keyfun() ::
    fun((tuple()) -> binary()).
-type appdefinable_headfun() ::
    fun((object_tag(), object_metadata()) -> head()).
-type appdefinable_metadatafun() ::
    fun(({leveled_codec:tag(), non_neg_integer(), any()}) ->
        {object_metadata(), list(erlang:timestamp())}).
-type appdefinable_indexspecsfun() ::
    fun((object_tag(), object_metadata(), object_metadata()|not_present) ->
        leveled_codec:index_specs()).
-type appdefinable_function_fun() ::
    appdefinable_keyfun() | appdefinable_headfun() |
    appdefinable_metadatafun() | appdefinable_indexspecsfun().
-type appdefinable_function_tuple() ::
    {appdefinable_function(), appdefinable_function_fun()}.

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().

-type head() ::
    binary()|tuple().
    % TODO:
    % This is currently not always a binary.  Wish is to migrate this so that
    % it is predictably a binary


-export_type([object_tag/0,
                headonly_tag/0,
                head/0,
                object_metadata/0,
                appdefinable_function_tuple/0]).

%%%============================================================================
%%% Mutable External Functions
%%%============================================================================

-spec key_to_canonicalbinary(tuple()) -> binary().
%% @doc
%% Convert a key to a binary in a consistent way for the tag.  The binary will
%% then be used to create the hash
key_to_canonicalbinary({?RIAK_TAG, Bucket, Key, null}) 
                                    when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>;
key_to_canonicalbinary({?RIAK_TAG, {BucketType, Bucket}, Key, SubKey})
                            when is_binary(BucketType), is_binary(Bucket) ->
    key_to_canonicalbinary({?RIAK_TAG,
                            <<BucketType/binary, Bucket/binary>>,
                            Key,
                            SubKey});
key_to_canonicalbinary(Key) when element(1, Key) == ?STD_TAG ->
    default_key_to_canonicalbinary(Key);
key_to_canonicalbinary(Key) ->
    OverrideFun =
        get_appdefined_function(key_to_canonicalbinary, 
                                    fun default_key_to_canonicalbinary/1,
                                    1),
    OverrideFun(Key).
    
default_key_to_canonicalbinary(Key) ->
    leveled_util:t2b(Key).


-spec build_head(object_tag()|headonly_tag(), object_metadata()) -> head().
%% @doc
%% Return the object metadata as a binary to be the "head" of the object
build_head(?HEAD_TAG, Value) ->
    % Metadata is not extracted with head objects, the head response is
    % just the unfiltered value that was input.  
    default_build_head(?HEAD_TAG, Value);
build_head(?RIAK_TAG, Metadata) ->
    {SibData, Vclock, _Hash, _Size} = Metadata,
    riak_metadata_to_binary(Vclock, SibData);
build_head(?STD_TAG, Metadata) ->
    default_build_head(?STD_TAG, Metadata);
build_head(Tag, Metadata) ->
    OverrideFun =
        get_appdefined_function(build_head, 
                                fun default_build_head/2,
                                2),
    OverrideFun(Tag, Metadata).

default_build_head(_Tag, Metadata) ->
    Metadata.


-spec extract_metadata(object_tag(), non_neg_integer(), any())
                            -> {object_metadata(), list(erlang:timestamp())}.
%% @doc
%% Take the inbound object and extract from it the metadata to be stored within
%% the ledger (and ultimately returned from a leveled_boookie:book_head/4
%% request (after conversion using build_head/2).
%%
%% As part of the response also return a list of last_modification_dates
%% associated with the object - with those dates being expressed as erlang
%% timestamps.
%%
%% The Object Size passed in to this function is as calculated when writing
%% the object to the Journal.  It may be recalculated here, if an alternative
%% view of size is required within the header
%%
%% Note objects with a ?HEAD_TAG should never be passed, as there is no
extract_metadata(?RIAK_TAG, SizeAsStoredInJournal, RiakObj) ->
    riak_extract_metadata(RiakObj, SizeAsStoredInJournal);
extract_metadata(?STD_TAG, SizeAsStoredInJournal, Obj) ->
    default_extract_metadata(?STD_TAG, SizeAsStoredInJournal, Obj);
extract_metadata(Tag, SizeAsStoredInJournal, Obj) ->
    OverrideFun =
        get_appdefined_function(extract_metadata, 
                                fun default_extract_metadata/3,
                                3),
    OverrideFun(Tag, SizeAsStoredInJournal, Obj).

default_extract_metadata(_Tag, SizeAsStoredInJournal, Obj) ->
    {{standard_hash(Obj), SizeAsStoredInJournal, undefined}, []}.


-spec diff_indexspecs(object_tag(),
                        object_metadata(),
                        object_metadata()|not_present)
                            -> leveled_codec:index_specs().
%% @doc
%% Take an object metadata part from within the journal, and an object metadata
%% part from the ledger (which should have a lower SQN), and generate index
%% specs by determining the difference between the index specs on the object
%% to be loaded and that on object already stored.
%%
%% This is only relevant where the journal compaction strategy of `recalc` is
%% used, the Keychanges will be used when `retain` is the compaction strategy
diff_indexspecs(?RIAK_TAG, UpdatedMetadata, OldMetadata) ->
    UpdIndexes =
        get_indexes_from_siblingmetabin(element(1, UpdatedMetadata), []),
    OldIndexes =
        case OldMetadata of
            not_present ->
                [];
            _ ->
                get_indexes_from_siblingmetabin(element(1, OldMetadata), [])
        end,
    diff_index_data(OldIndexes, UpdIndexes);
diff_indexspecs(?STD_TAG, UpdatedMetadata, CurrentMetadata) ->
    default_diff_indexspecs(?STD_TAG, UpdatedMetadata, CurrentMetadata);
diff_indexspecs(Tag, UpdatedMetadata, CurrentMetadata) ->
    OverrideFun =
        get_appdefined_function(diff_indexspecs, 
                                fun default_diff_indexspecs/3,
                                3),
    OverrideFun(Tag, UpdatedMetadata, CurrentMetadata).

default_diff_indexspecs(_Tag, _UpdatedMetadata, _CurrentMetadata) ->
    [].

%%%============================================================================
%%% Standard External Functions
%%%============================================================================

-spec defined_objecttags() -> list(object_tag()).
%% @doc
%% Return the list of object tags
defined_objecttags() ->
    [?STD_TAG, ?RIAK_TAG].


-spec default_reload_strategy(object_tag())
                                    -> {object_tag(), 
                                        leveled_codec:compaction_method()}.
%% @doc
%% State the compaction_method to be used when reloading the Ledger from the
%% journal for each object tag.  Note, no compaction strategy required for 
%% head_only tag
default_reload_strategy(Tag) ->
    {Tag, retain}.


-spec get_size(object_tag()|headonly_tag(), object_metadata())
                                                    -> non_neg_integer().
%% @doc
%% Fetch the size from the metadata
get_size(?RIAK_TAG, RiakObjectMetadata) ->
    element(4, RiakObjectMetadata);
get_size(_Tag, ObjectMetadata) ->
    element(2, ObjectMetadata).


-spec get_hash(object_tag()|headonly_tag(), object_metadata())
                                                    -> non_neg_integer().
%% @doc
%% Fetch the hash from the metadata
get_hash(?RIAK_TAG, RiakObjectMetadata) ->
    element(3, RiakObjectMetadata);
get_hash(_Tag, ObjectMetadata) ->
    element(1, ObjectMetadata).

-spec standard_hash(any()) -> non_neg_integer().
%% @doc
%% Hash the whole object
standard_hash(Obj) ->
    erlang:phash2(term_to_binary(Obj)).


%%%============================================================================
%%% Handling Override Functions
%%%============================================================================

-spec get_appdefined_function(
    appdefinable_function(), appdefinable_function_fun(), non_neg_integer()) ->
        appdefinable_function_fun().
%% @doc
%% If a keylist of [{function_name, fun()}] has been set as an environment 
%% variable for a tag, then this FunctionName can be used instead of the
%% default
get_appdefined_function(FunctionName, DefaultFun, RequiredArity) ->
    case application:get_env(leveled, FunctionName) of
        undefined ->
            DefaultFun;
        {ok, Fun} when is_function(Fun, RequiredArity) ->
            Fun
    end.

%%%============================================================================
%%% Tag-specific Internal Functions
%%%============================================================================


-spec riak_extract_metadata(binary()|delete, non_neg_integer()) -> 
                            {riak_metadata(), list()}.
%% @doc
%% Riak extract metadata should extract a metadata object which is a 
%% five-tuple of:
%% - Binary of sibling Metadata
%% - Binary of vector clock metadata
%% - Non-exportable hash of the vector clock metadata
%% - The largest last modified date of the object
%% - Size of the object
%%
%% The metadata object should be returned with the full list of last 
%% modified dates (which will be used for recent anti-entropy index creation)
riak_extract_metadata(delete, Size) ->
    {{delete, null, null, Size}, []};
riak_extract_metadata(ObjBin, Size) ->
    {VclockBin, SibBin, LastMods} = riak_metadata_from_binary(ObjBin),
    {{binary:copy(SibBin), 
            binary:copy(VclockBin), 
            erlang:phash2(lists:sort(binary_to_term(VclockBin))), 
            Size},
        LastMods}.

%% <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
%%%     VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

riak_metadata_to_binary(VclockBin, SibMetaBin) ->
    VclockLen = byte_size(VclockBin),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer,
        VclockLen:32/integer, VclockBin/binary,
        SibMetaBin/binary>>.
    
riak_metadata_from_binary(V1Binary) ->
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer,
            Rest/binary>> = V1Binary,
    <<VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> = Rest,
    {SibMetaBin, LastMods} =
        case SibCount of
            SC when is_integer(SC) ->
                get_metadata_from_siblings(SibsBin,
                                            SibCount,
                                            <<SibCount:32/integer>>,
                                            [])
        end,
    {VclockBin, SibMetaBin, LastMods}.

get_metadata_from_siblings(<<>>, 0, SibMetaBin, LastMods) ->
    {SibMetaBin, LastMods};
get_metadata_from_siblings(<<ValLen:32/integer, Rest0/binary>>,
                            SibCount,
                            SibMetaBin,
                            LastMods) ->
    <<_ValBin:ValLen/binary, MetaLen:32/integer, Rest1/binary>> = Rest0,
    <<MetaBin:MetaLen/binary, Rest2/binary>> = Rest1,
    LastMod =
        case MetaBin of 
            <<MegaSec:32/integer,
                Sec:32/integer,
                MicroSec:32/integer,
                _Rest/binary>> ->
                    {MegaSec, Sec, MicroSec};
            _ ->
                {0, 0, 0}
        end,
    get_metadata_from_siblings(Rest2,
                                SibCount - 1,
                                <<SibMetaBin/binary,
                                    0:32/integer,
                                    MetaLen:32/integer,
                                    MetaBin:MetaLen/binary>>,
                                    [LastMod|LastMods]).


get_indexes_from_siblingmetabin(<<0:32/integer,
                                        MetaLen:32/integer,
                                        MetaBin:MetaLen/binary,
                                        RestBin/binary>>,
                                    Indexes) ->
    UpdIndexes = lists:umerge(get_indexes_frommetabin(MetaBin), Indexes),
    get_indexes_from_siblingmetabin(RestBin, UpdIndexes);
get_indexes_from_siblingmetabin(<<SibCount:32/integer, RestBin/binary>>,
                                    Indexes) when SibCount > 0 ->
    get_indexes_from_siblingmetabin(RestBin, Indexes);
get_indexes_from_siblingmetabin(_, Indexes) ->
    Indexes.


%% @doc
%% Parse the metabinary for an individual sibling and return a list of index
%% entries.
get_indexes_frommetabin(<<_LMD1:32/integer, _LMD2:32/integer, _LMD3:32/integer,
                                VTagLen:8/integer, _VTag:VTagLen/binary,
                                Deleted:1/binary-unit:8,
                                MetaRestBin/binary>>) when Deleted /= <<1>> ->
    lists:usort(indexes_of_metabinary(MetaRestBin));
get_indexes_frommetabin(_) ->
    [].


indexes_of_metabinary(<<>>) ->
    [];
indexes_of_metabinary(<<KeyLen:32/integer, KeyBin:KeyLen/binary,
                        ValueLen:32/integer, ValueBin:ValueLen/binary,
                        Rest/binary>>) ->
    Key = decode_maybe_binary(KeyBin),
    case Key of
        <<"index">> ->
            Value = decode_maybe_binary(ValueBin),
            Value;
        _ ->
            indexes_of_metabinary(Rest)
    end.


decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin);
decode_maybe_binary(<<_Other:8, Bin/binary>>) ->
    Bin.

-spec diff_index_data(
    [{binary(), index_value()}], [{binary(), index_value()}]) ->
        [{index_op(), binary(), index_value()}].
diff_index_data(OldIndexes, AllIndexes) ->
    OldIndexSet = ordsets:from_list(OldIndexes),
    AllIndexSet = ordsets:from_list(AllIndexes),
    diff_specs_core(AllIndexSet, OldIndexSet).


diff_specs_core(AllIndexSet, OldIndexSet) ->
    NewIndexSet = ordsets:subtract(AllIndexSet, OldIndexSet),
    RemoveIndexSet =
        ordsets:subtract(OldIndexSet, AllIndexSet),
    NewIndexSpecs =
        assemble_index_specs(
            ordsets:subtract(NewIndexSet, OldIndexSet),
            add
        ),
    RemoveIndexSpecs =
        assemble_index_specs(RemoveIndexSet, remove),
    NewIndexSpecs ++ RemoveIndexSpecs.

%% @doc Assemble a list of index specs in the
%% form of triplets of the form
%% {IndexOperation, IndexField, IndexValue}.
-spec assemble_index_specs(
    [{binary(), binary()}], index_op()) ->
        [{index_op(), binary(), binary()}].
assemble_index_specs(Indexes, IndexOp) ->
    [{IndexOp, Index, Value} || {Index, Value} <- Indexes].


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

index_extract_test() ->
    SibMetaBin =
        <<0,0,0,1,0,0,0,0,0,0,0,221,0,0,6,48,0,4,130,247,0,1,250,134,
            1,101,0,0,0,0,4,1,77,68,75,0,0,0,44,0,131,107,0,39,77,68,
            86,101,49,55,52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,
            45,97,53,102,50,45,53,54,98,51,98,97,57,57,99,55,56,50,0,0,
            0,6,1,105,110,100,101,120,0,0,0,79,0,131,108,0,0,0,2,104,2,
            107,0,8,105,100,120,49,95,98,105,110,107,0,20,50,49,53,50,
            49,49,48,55,50,51,49,55,51,48,83,111,112,104,105,97,104,2,
            107,0,8,105,100,120,49,95,98,105,110,107,0,19,50,49,56,50,
            48,53,49,48,49,51,48,49,52,54,65,118,101,114,121,106,0,0,0,
            5,1,77,68,75,50,0,0,0,44,0,131,107,0,39,77,68,86,101,49,55,
            52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,45,97,53,102,
            50,45,53,54,98,51,98,97,57,57,99,55,56,50>>,
    Indexes = get_indexes_from_siblingmetabin(SibMetaBin, []),
    ExpIndexes = [{"idx1_bin","21521107231730Sophia"},
                    {"idx1_bin","21820510130146Avery"}],
    ?assertMatch(ExpIndexes, Indexes),
    SibMetaBinNoIdx =
        <<0,0,0,1,0,0,0,0,0,0,0,128,0,0,6,48,0,4,130,247,0,1,250,134,
            1,101,0,0,0,0,4,1,77,68,75,0,0,0,44,0,131,107,0,39,77,68,
            86,101,49,55,52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,
            45,97,53,102,50,45,53,54,98,51,98,97,57,57,99,55,56,50,0,0,0,
            5,1,77,68,75,50,0,0,0,44,0,131,107,0,39,77,68,86,101,49,55,
            52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,45,97,53,102,
            50,45,53,54,98,51,98,97,57,57,99,55,56,50>>,
    ?assertMatch([], get_indexes_from_siblingmetabin(SibMetaBinNoIdx, [])),
    SibMetaBinOverhang =
        <<0,0,0,1,0,0,0,0,0,0,0,221,0,0,6,48,0,4,130,247,0,1,250,134,
            1,101,0,0,0,0,4,1,77,68,75,0,0,0,44,0,131,107,0,39,77,68,
            86,101,49,55,52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,
            45,97,53,102,50,45,53,54,98,51,98,97,57,57,99,55,56,50,0,0,
            0,6,1,105,110,100,101,120,0,0,0,79,0,131,108,0,0,0,2,104,2,
            107,0,8,105,100,120,49,95,98,105,110,107,0,20,50,49,53,50,
            49,49,48,55,50,51,49,55,51,48,83,111,112,104,105,97,104,2,
            107,0,8,105,100,120,49,95,98,105,110,107,0,19,50,49,56,50,
            48,53,49,48,49,51,48,49,52,54,65,118,101,114,121,106,0,0,0,
            5,1,77,68,75,50,0,0,0,44,0,131,107,0,39,77,68,86,101,49,55,
            52,55,48,50,55,45,54,50,99,49,45,52,48,57,55,45,97,53,102,
            50,45,53,54,98,51,98,97,57,57,99,55,56,50,0,0,0,0,0,0,0,4,
            0,0,0,0>>,
    ?assertMatch(ExpIndexes,
                    get_indexes_from_siblingmetabin(SibMetaBinOverhang, [])).

diff_index_test() ->
    UpdIndexes =
        [{<<"idx1_bin">>,<<"20840930001702Zoe">>},
            {<<"idx1_bin">>,<<"20931011172606Emily">>}],
    OldIndexes =
        [{<<"idx1_bin">>,<<"20231126131808Madison">>},
            {<<"idx1_bin">>,<<"20931011172606Emily">>}],
    IdxSpecs = diff_index_data(OldIndexes, UpdIndexes),
    ?assertMatch([{add, <<"idx1_bin">>, <<"20840930001702Zoe">>},
                    {remove, <<"idx1_bin">>,<<"20231126131808Madison">>}], IdxSpecs).

decode_test() ->
    Bin = <<"999">>,
    BinTerm = term_to_binary("999"),
    ?assertMatch("999", binary_to_list(
                            decode_maybe_binary(<<1:8/integer, Bin/binary>>))),
    ?assertMatch("999", decode_maybe_binary(<<0:8/integer, BinTerm/binary>>)),
    ?assertMatch("999", binary_to_list(
                            decode_maybe_binary(<<2:8/integer, Bin/binary>>))).

-endif.