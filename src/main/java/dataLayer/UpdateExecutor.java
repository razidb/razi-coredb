package dataLayer;

import collection.CollectionController;
import controller.DBController;

public class UpdateExecutor {
    /* Responsible for executing optimized update operations over a set of documents */
    private String db_name;
    private String collection_name;
    private CollectionController collectionController;

    public UpdateExecutor(String db_name, String collection_name){
        this.db_name = db_name;
        this.collection_name = collection_name;
        DBController dbController = DBController.getDbController();
        this.collectionController = dbController.getDatabase(db_name).getCollection(collection_name).getController();
    }

    public int execute(Object data, String dataType){
        boolean replace = false;

        // 1- Get documents to be updated
        //      - it could be helpful to use existing selection executor

        // 2- Perform update on every index that contains the updated values

        // 3- A one-time-go to disk and perform update operations there as well

        // 4- return the number of updated documents

        // Restrictions:
        //  - refuse update on `_id` field

        // TODO: implement functionality

        return 0;

    }
}
