import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import { DeleteBackupCommand, DeleteItemCommand, DeleteItemCommandInput, DynamoDBClient, GetItemCommand, GetItemCommandInput, PutItemCommand, PutItemCommandInput, ScanCommand, ScanCommandInput } from '@aws-sdk/client-dynamodb'
import { marshall } from "@aws-sdk/util-dynamodb";
import * as yup from "yup";
import { v4 } from "uuid";

const docClient = new DynamoDBClient({ region: 'us-east-1' });
const tableName = "ProductsTable";


const headers = {
  "content-type": "application/json",
};

const schema = yup.object().shape({
  name: yup.string().required(),
});
/**
 * 
 * @param event 
 * @returns 
 * https://catalog.us-east-1.prod.workshops.aws/workshops/56ef6f79-74e2-4710-aefb-10b9807057a9/en-US/persisting-data/dynamodb/crud
 * 
 */

export const helloWorld = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ "message": "Hello World, Dorothi" }),
  }
}

export const createProduct = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {

  /**
   * Running locally
   * https://forum.serverless.com/t/accessing-event-body-in-serverless-create-template-hello-world/8220
   */
  try {
    const reqBody = (typeof event.body === "string") ? JSON.parse(event.body as string) : event.body;
    console.log("reqBody");
    console.log(reqBody);
    // await schema.validate(reqBody, { abortEarly: false });

    const product = {
      ...reqBody,
      productID: v4(),
    }

    const params: PutItemCommandInput = {
      TableName: tableName,
      Item: marshall(product)
    }

    await docClient.send(new PutItemCommand(params));
    return {
      statusCode: 201,
      headers,
      body: JSON.stringify(
        product
      ),
    };
  }
  catch (err) {
    return handleError(err);
  }

};

const fetchProductById = async (id: string) => {
  const params: GetItemCommandInput = {
    TableName: tableName,
    Key: {
      'productID': { S: id }
    }
  }
  const output = await docClient.send(new GetItemCommand(params))

  if (!output.Item) {
    throw new HttpError(404, { error: "not found" });
  }

  return output.Item;
};


export const getProduct = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const product = await fetchProductById(event.pathParameters?.id as string);
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(product),
    };
  } catch (err) {
    return handleError(err);
  }

}

/**
 * 
 * @param event 
 * @returns 
 * Updating an item is identical to creating an item
 */
export const updateProduct = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const id = event.pathParameters?.id as string;

    await fetchProductById(id);

    const reqBody = (typeof event.body === "string") ? JSON.parse(event.body as string) : event.body;;

    await schema.validate(reqBody, { abortEarly: false });

    const product = {
      ...reqBody,
      productID: id,
    };

    const params: PutItemCommandInput = {
      TableName: tableName,
      Item: marshall(product)
    }

    await docClient.send(new PutItemCommand(params))
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(product),
    };
  } catch (error) {
    return handleError(error);
  }

}

export const deleteProduct = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const id = event.pathParameters?.id as string;

    await fetchProductById(id);
    const params: DeleteItemCommandInput = {
      TableName: tableName,
      Key: {
        'productID': { S: id }
      }
    }
    await docClient.send(new DeleteItemCommand(params));
    return {
      statusCode: 204,
      body: "",
    };
  } catch (error) {
    return handleError(error);
  }
}

export const listProducts = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  const params: ScanCommandInput = {
    TableName: tableName,
  }
  const output = await docClient.send(new ScanCommand(params));
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify(output.Items),
  };
}

class HttpError extends Error {
  constructor(public statusCode: number, body: Record<string, unknown> = {}) {
    super(JSON.stringify(body));
  }
}

const handleError = (e: unknown) => {
  if (e instanceof yup.ValidationError) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({
        errors: e.errors,
      }),
    };
  }

  if (e instanceof SyntaxError) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ error: `invalid request body format : "${e.message}"` }),
    };
  }

  if (e instanceof HttpError) {
    return {
      statusCode: e.statusCode,
      headers,
      body: e.message,
    };
  }

  throw e;
};