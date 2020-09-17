import numpy as np
import pandas as pd
import uuid
import os
import time
import json

class Provenance:
    
    # Constants:
    NAMESPACE_FUNC = 'activity:'
    NAMESPACE_ENTITY = 'entity:'
    INPUT = 'input'
    OUTPUT = 'output'
    
    CHUNK_SIZE = 60000
    
    # PROV-N objects
    ENTITY = 'prov:entity'
    GENERATED_ENTITY = 'prov:generatedEntity'
    USED_ENTITY = 'prov:usedEntity'
    ACTIVITY = 'prov:activity'
    
    # PROV-N relations
    GENERATION = 'wasGeneratedBy'
    USE = 'used'
    DERIVATION = 'wasDerivedFrom'
    INVALIDATION = 'wasInvalidatedBy'

    DEFAULT_PATH = 'prov_results/'
    
    def __init__(self, df, results_path=None):
        # Inizialize provenance activities, relations and new entities
        self.current_act = []
        self.current_relations = []
        self.new_entities = []
        
        # Initialize operation number:
        self.operation_number = -1
        self.instance = self.OUTPUT + str(self.operation_number)
        
         # Set input dataframe parameters:
        self.current_m, self.current_n = df.shape
        self.current_columns = df.columns
        self.current_index = df.index

        # Set results path:
        self.results_path = DEFAULT_PATH + time.strftime('%Y%m%d-%H%M%S') if results_path is None else results_path
        
        # Create provenance entities of the input dataframe:
        self.current_ent = self.create_prov_entities(df, self.INPUT)
        
        # Save input provenance document
        #self.save_json_prov(os.path.join(self.results_path, self.INPUT))

    def timing(f):
        def wrap(*args):
            # Get timing of provenance function:
            time1 = time.time()
            ret = f(*args)
            time2 = time.time()
            #text = '{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0)
            text = '{:s} function took {:.3f} sec.'.format(f.__name__, (time2-time1))
            print(text)
            
            self = args[0]

            # Get new folder size
            #new_folder_name = self.INPUT if self.operation_number == -1 else self.OUTPUT + str(self.operation_number)
            #new_folder_path = os.path.join(self.results_path, new_folder_name) 
            #total = 0
            #for file in os.scandir(new_folder_path):
            #    if file.is_file():
            #        total += file.stat().st_size

            #size = get_size_format(total)

            # Create folder if not exists
            nameFile = os.path.join(self.results_path, self.INPUT)
            if not os.path.exists(nameFile):
                os.makedirs(nameFile)

            # Save infos in log file
            pipeline_path = os.path.join(self.results_path, 'log_file.txt')
            with open(pipeline_path, 'a+') as log_file:
                log_file.write('[' + time.strftime("%d/%m-%H:%M:%S") +']' + text + '\n')
                #log_file.write(new_folder_name +' folder size: ' + str(size) + '\n')

            #duration = time2 - time1
            #print(f.__name__
            #      + ' finished in ' 
            #      + time.strftime('%H:%M:%S', time.gmtime(duration)))

            return ret
        return wrap
        
    def create_entity(self, ent_id, record_id, value, feature_name, index, instance):
        """Create a provenance entity.
        Return a dictionary with the id and the attributes of the entity."""
        # Get attributes:
        other_attributes = {}
        other_attributes['record_id'] = record_id
        other_attributes['value'] = value
        other_attributes['feature_name'] = feature_name
        other_attributes['index'] = str(index)
        other_attributes['instance'] = str(instance)
        
        # Add entity to new numpy array entities:
        ent = {'identifier': ent_id, 'attributes': other_attributes}
        self.new_entities.append(ent)
        
        return ent
    
    def create_activity(self, function_name, features_name=None, description=None, other_attributes=None):
        """Create a provenance activity and add to the current activities array.
        Return the id of the new prov activity."""
        # Get default activity attributes:
        attributes = {}
        attributes['function_name'] = function_name
        if features_name is not None:
            attributes['features_name'] =  features_name
        if description is not None:
            attributes['description'] =  description
        attributes['operation_number'] = str(self.operation_number)
        
        # Join default and extra attributes:
        if other_attributes is not None:
            attributes.update(other_attributes)
            
        act_id = self.NAMESPACE_FUNC + str(uuid.uuid4())
        
        # Add activity to current provenance document:
        act = {'identifier': act_id, 'attributes': attributes}
        self.current_act.append(act)
        
        return act_id
    
    def create_relation(self, relation_type, **relation):
        """Add a relation to the current relations array.
        Return the new relation."""
        if relation_type == self.GENERATION:
            relation[self.ENTITY] = relation.pop('a')
            relation[self.ACTIVITY] = relation.pop('b')
        elif relation_type == self.USE:
            relation[self.ACTIVITY] = relation.pop('a')
            relation[self.ENTITY] = relation.pop('b')
        elif relation_type == self.DERIVATION:
            relation[self.GENERATED_ENTITY] = relation.pop('a')
            relation[self.USED_ENTITY] = relation.pop('b')
        elif relation_type == self.INVALIDATION:
            relation[self.ENTITY] = relation.pop('a')
            relation[self.ACTIVITY] = relation.pop('b')
        #else:
            #TODO: Exception, invalid relation
            
        relation.update({'prov:relation_type':relation_type})
        self.current_relations.append(relation)
        
        return relation
        
    @timing
    def create_prov_entities(self, dataframe, instance=None):
        """Return a numpy array of new provenance entities related to the dataframe."""
        instance = self.instance if instance is None else instance
        columns = dataframe.columns
        indexes = dataframe.index
        tot_ent = len(columns) * len(indexes)

        """Save provenance in json file."""
        nameFile = os.path.join(self.results_path, self.INPUT)
        if not os.path.exists(nameFile):
            os.makedirs(nameFile)
        
        ents_path = os.path.join(nameFile, 'entities')
            
        # Create output array of entities:
        from_ent = 0
        num_ent = 0
        entities = np.empty(dataframe.shape, dtype=object)
        for i in range(self.current_m):
            record_id = str(uuid.uuid4())
            for j in range(self.current_n):
                ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                value = str(dataframe.iat[i, j])
                 # Add entity to current provenance document:
                entities[i][j] = self.create_entity(ent_id, record_id, value, columns[j], indexes[i], self.operation_number)

                #Save input entities in json files
                num_ent += 1
                if(num_ent is not 0 and (num_ent % self.CHUNK_SIZE == 0 or num_ent == tot_ent)):
                    output_name = ents_path + '.json' if num_ent<=self.CHUNK_SIZE else ents_path + '_' + str(num_ent//self.CHUNK_SIZE) + '.json'
                    with open(output_name, 'w', encoding='utf-8') as ents_file:
                        ents = self.new_entities[from_ent:num_ent+1]
                        from_ent = num_ent
                        json.dump(ents, ents_file, ensure_ascii=False, indent=4)

        return entities
    
    def set_current_values(self, dataframe, entities_out):
        """Update values of current entities after every operation."""
        # Set output dataframe entities:
        self.current_m, self.current_n = dataframe.shape
        self.current_columns = dataframe.columns
        self.current_index = dataframe.index
        self.current_ent = entities_out
        
    def initialize(self):
        self.current_act = []
        self.current_relations = []
        self.new_entities = []

        # Increment operation number:
        self.operation_number += 1
        self.instance = self.OUTPUT + str(self.operation_number)
            
    def save_json_prov(self, nameFile):
        """Save provenance in json file."""
        if not os.path.exists(nameFile):
            os.makedirs(nameFile)
        
        ents_path = os.path.join(nameFile, 'entities')
        acts_path = os.path.join(nameFile, 'activities.json')
        rel_path = os.path.join(nameFile, 'relations')
                
        # Save entities: 
        #entities = list(self.current_ent.flatten())
        entities = self.new_entities
        
        if entities:
            for i in range(0, len(entities), self.CHUNK_SIZE):
                output_name = ents_path + '.json' if i//self.CHUNK_SIZE == 0 else ents_path + '_' + str(i//self.CHUNK_SIZE) + '.json'
                with open(output_name, 'w', encoding='utf-8') as ents_file:
                    ents = entities[i:i+self.CHUNK_SIZE]
                    json.dump(ents, ents_file, ensure_ascii=False, indent=4)
                
        # Save activities:
        if self.current_act:
            with open(acts_path, 'w', encoding='utf-8') as acts_file:
                json.dump(self.current_act, acts_file, ensure_ascii=False, indent=4)
                
        # Save all relations:
        if self.current_relations:
            for i in range(0, len(self.current_relations), self.CHUNK_SIZE):
                output_name = rel_path + '.json' if i//self.CHUNK_SIZE == 0 else rel_path + '_' + str(i//self.CHUNK_SIZE) + '.json'
                with open(output_name, 'w', encoding='utf-8') as rel_file:
                    rels = self.current_relations[i:i+self.CHUNK_SIZE]
                    json.dump(rels, rel_file, ensure_ascii=False, indent=4)
        
    ###
    ###  PROVENANCE METHODS
    ###
    
    @timing
    def get_prov_feature_transformation(self, df_out, columnsName, description=None):
        """Return provenance document related to features trasformation function.
        
        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of transformed columns name
        """
        function_name = 'Feature Transformation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description)
            col_index = columns_out.get_loc(col_name)
            for i in range(self.current_m):
                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['identifier']
                record_id = e_in['attributes']['record_id']
                value = str(df_out.iat[i, col_index])
                
                # Create a new entity with new value:
                ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                e_out = self.create_entity(ent_id, record_id, value, col_name, indexes_out[i], self.operation_number)
                e_out_identifier = e_out['identifier']
                    
                self.create_relation(self.GENERATION, a=e_out_identifier, b=act_id)
                self.create_relation(self.USE, a=act_id, b=e_in_identifier)
                self.create_relation(self.DERIVATION, a=e_out_identifier, b=e_in_identifier)
                self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)
                
                entities_in[i][col_index] = e_out
                
        # Update current values:
        self.set_current_values(df_out, entities_in)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        
        return self

    @timing
    def get_prov_space_transformation(self, df_out, columnsName, description=None):
        """Return provenance document related to space trasformation function.
        
        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of columns name joined to create the new column
        """
        function_name = 'Space Transformation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        m, n = self.current_m, self.current_n
        columns_in = self.current_columns

        # Output values:
        m_new, n_new = df_out.shape
        columns_out = df_out.columns
        indexes_out = df_out.index
        
        # Create entities of the output dataframe:
        entities_out = np.empty(df_out.shape, dtype=object)
        
        # Get feature indexes used for space transformation:
        indexes = []
        for feature in columnsName:
            indexes.append(columns_in.get_loc(feature))

        # Get feature indexes generated by space transformation:
        indexes_new = []
        for feature in columns_out:
            if feature not in columns_in:
                indexes_new.append(columns_out.get_loc(feature))
            
        # Create space transformation activity:
        act_id = self.create_activity(function_name, ', '.join(columnsName), description)
        
        # Get provenance related to the new column:
        for i in range(m):
            first_ent = entities_in[i][0]
            record_id = first_ent['attributes']['record_id']
            for j in indexes_new:
                value = str(df_out.iat[i, j])
                ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                e_out = self.create_entity(ent_id, record_id, value, columns_out[j], indexes_out[i], self.operation_number)
                e_out_identifier = e_out['identifier']
                entities_out[i][j] = e_out
                self.create_relation(self.GENERATION, a=e_out_identifier, b=act_id)
                for index in indexes:
                    e_in = entities_in[i][index]
                    e_in_identifier = e_in['identifier']
                    self.create_relation(self.USE, a=act_id, b=ent_id)
                    self.create_relation(self.DERIVATION, a=e_out_identifier, b=e_in_identifier)
                    if columns_in[index] not in columns_out:
                        self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)

        # Rearrange unchanged columns:
        for col_name in columns_out:
            if col_name in columns_in:
                old_j = columns_in.get_loc(col_name)
                new_j = columns_out.get_loc(col_name)
                entities_out[:,new_j] = entities_in[:,old_j]
                
        # Update current values:
        self.set_current_values(df_out, entities_out)
        
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self
    
    @timing
    def get_prov_dim_reduction(self, df_out, description=None):
        """Return provenance document related to selection or projection."""
        function_name = 'Dimensionality reduction'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        columns_in = self.current_columns
        index_in = self.current_index
        m, n = self.current_m, self.current_n
        
        # Output values:
        columns_out = df_out.columns
        index_out = df_out.index
        m_new, n_new = df_out.shape
        
        delColumnsName = set(columns_in) - set(columns_out) # List of deleted columns
        delIndex = set(index_in) - set(index_out) # List of deleted columns
        
        # Create selection activity:
        act_id = self.create_activity(function_name, ', '.join(delColumnsName), description)
        
        for i in delIndex:
            for j in range(n):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['identifier']
                self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)
        
        delColumns = []
        for colName in delColumnsName:
            j = columns_in.get_loc(colName)
            delColumns.append(j)
            for i in range(m):
                e_in = entities_in[i][j]
                e_in_identifier = e_in['identifier']
                self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)
        
        entities_in = np.delete(entities_in, list(delIndex), axis=0)
        entities_out = np.delete(entities_in, delColumns, axis=1)
        
        # Update current values:
        self.set_current_values(df_out, entities_out)
        
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self

    @timing
    def get_prov_instance_generation(self, df_out, columnsName, description=None):
        """Return provenance document related to instance generation function."""
        function_name = 'Instance Generation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        m, n = self.current_m, self.current_n
        
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        m_new, n_new = df_out.shape
        
        # Create numpy array of new entities:
        new_entities = np.empty((m_new-m, n), dtype=object)

        acts = {}
        # Provenance of existent data
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description)
            acts[col_name] = act_id
            col_index = columns_out.get_loc(col_name)
            for i in range(m):
                e_in = entities_in[i][col_index]
                ent_id = e_in['identifier']
                self.create_relation(self.USE, a=act_id, b=ent_id)

        columnsName_out = set(columns_out) - set(columnsName) # List of non selected columns
        if columnsName_out:
            defaultAct_id = self.create_activity(function_name, None, description)
        
        # Provenance of new data
        for i in range(m, m_new):
            record_id = str(uuid.uuid4())
            for j in range(n):
                col_name = columns_out[j]
                act_id = acts[col_name] if col_name in acts else defaultAct_id #TODO
                value = str(df_out.iat[i, j])
                ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                e_out = self.create_entity(ent_id, record_id, value, col_name, indexes_out[i], self.operation_number)
                e_out_identifier = e_out['identifier']
                new_entities[i-m][j] = e_out
                self.create_relation(self.GENERATION, a=e_out_identifier, b=act_id)
                    
        entities_out = np.concatenate((entities_in, new_entities), axis=0)
        
        # Update current values:
        self.set_current_values(df_out, entities_out)
        
        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self
    
    @timing
    def get_prov_value_transformation(self, df_out, columnsName, description=None):
        """Return provenance document related to value transformation function.
        Used when a value inside the dataframe is replaced.
        
        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of column names where the value transformation is applied
        """
        function_name = 'Value Transformation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent

        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        
        for col_name in columnsName:
            add_act = True
            col_index = columns_out.get_loc(col_name)
            for i in range(self.current_m):
                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['identifier']
                record_id = e_in['attributes']['record_id']
                val_in = e_in['attributes']['value']
                
                value = str(df_out.iat[i, col_index])
                
                # Check if the input value is the replaced value
                if str(val_in) != str(value):
                    if add_act:
                        # Create value transformation activity:
                        act_id = self.create_activity(function_name, col_name, description)
                        add_act = False
                    # Create new entity with the new value
                    ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                    e_out = self.create_entity(ent_id, record_id, value, col_name, indexes_out[i], self.operation_number)
                    e_out_identifier = e_out['identifier']
                    self.create_relation(self.GENERATION, a=e_out_identifier, b=act_id)
                    self.create_relation(self.USE, a=act_id, b=e_in_identifier)
                    self.create_relation(self.DERIVATION, a=e_out_identifier, b=e_in_identifier)
                    self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)
                    
                    entities_in[i][col_index] = e_out

        # Update current values:
        self.set_current_values(df_out, entities_in)

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))

        return self
    
    @timing
    def get_prov_imputation(self, df_out, columnsName, description=None):
        """Return provenance document related to imputation function.

        Keyword argument:
        df_out -- the output dataframe
        columnsName -- list of column names where the imputation is applied
        """
        function_name = 'Imputation'
        self.initialize()
        # Get current values:
        entities_in = self.current_ent
        # Output values:
        columns_out = df_out.columns
        indexes_out = df_out.index
        
            
        for col_name in columnsName:
            act_id = self.create_activity(function_name, col_name, description) 
            col_index = columns_out.get_loc(col_name)
            for i in range(self.current_m):
                value = str(df_out.iat[i, col_index])
                
                e_in = entities_in[i][col_index]
                e_in_identifier = e_in['identifier']
                record_id = e_in['attributes']['record_id']
                val_in = e_in['attributes']['value']
                
                if val_in == 'nan':
                    # Create new entity with the new value
                    ent_id = self.NAMESPACE_ENTITY + str(uuid.uuid4())
                    e_out = self.create_entity(ent_id, record_id, value, col_name, indexes_out[i], self.operation_number)
                    e_out_identifier = e_out['identifier']
                    self.create_relation(self.GENERATION, a=e_out_identifier, b=act_id)
                    self.create_relation(self.DERIVATION, a=e_out_identifier, b=e_in_identifier)
                    self.create_relation(self.INVALIDATION, a=e_in_identifier, b=act_id)

                    entities_in[i][col_index] = e_out
                else:
                    self.create_relation(self.USE, a=act_id, b=e_in_identifier)
                    

        # Save provenance document in json file:
        self.save_json_prov(os.path.join(self.results_path, self.instance))
        # Update current values:
        self.set_current_values(df_out, entities_in)

        return self

def get_size_format(b, factor=1024, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if b < 1024:
            return f'{b:.2f}{unit}{suffix}'
        b /= 1024
    return f'{b:.2f}Y{suffix}'