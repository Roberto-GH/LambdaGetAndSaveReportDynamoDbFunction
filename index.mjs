import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const client = new DynamoDBClient({ region: process.env.AWS_REGION });

const SUMMARY_TABLE_NAME = process.env.SUMMARY_TABLE_NAME;
const SUMMARY_TABLE_PK_VALUE = 1;

export const handler = async (event) => {
console.log('Evento recibido:', JSON.stringify(event, null, 2));
  for (const record of event.Records) {     
     if (record.eventName === 'INSERT') {
       try {         
         const newLoan = unmarshall(record.dynamodb.NewImage);
         console.log('Nuevo préstamo procesado:', newLoan);         
         const amount = parseFloat(newLoan.amount);         
         if (isNaN(amount)) {
           console.error('El monto no es un número válido:', newLoan.amount);
           continue; 
         }
        
         const params = {
           TableName: SUMMARY_TABLE_NAME,
           Key: {             
             'reportId': { 'N': String(SUMMARY_TABLE_PK_VALUE) }
           },           
           UpdateExpression:
             "SET totalAmount = if_not_exists(totalAmount, :zero) + :amount, " +
             "numberOfLoans = if_not_exists(numberOfLoans, :one) + :one, " +
             "lastUpdated = :now",
           ExpressionAttributeValues: {
             ':amount': { 'N': String(amount) },
             ':one': { 'N': '1' },    
             ':zero': { 'N': '0' },   
             ':now': { 'S': new Date().toISOString() }
           },                         
           ReturnValues: "UPDATED_NEW"
         };                           
                                      
         console.log('Ejecutando UpdateItem con los parámetros:', params);

         const command = new UpdateItemCommand(params);
         const result = await client.send(command);

         console.log('Tabla de resumen actualizada con éxito:', result);

       } catch (error) {
         console.error('Error al procesar el registro:', record);
         console.error(error);         
         throw error;
       }
     }
   }

   return {
     statusCode: 200,
     body: JSON.stringify(`Procesados ${event.Records.length} registros.`),
   };
   
 };
