// example.as
import { nx, console } from 'nx';

export function run(): void {
  console.log('Hello from AssemblyScript!');
  const keyValue = nx.getKeyValue('example');
  console.log(`Key-Value: ${keyValue}`);
}