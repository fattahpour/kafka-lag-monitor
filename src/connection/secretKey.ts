export function secretKey(profileName: string, field: string): string {
  return `kafkaLagMonitor.connection.${profileName}.${field}`;
}
