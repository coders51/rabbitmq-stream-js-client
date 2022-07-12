export class Producer {
  constructor(private stream: string, private publisherId: number, private publisherRef?: string) {
    console.log(
      `New producer created with steam name ${stream}, publisher id ${publisherId} and publisher reference ${publisherRef}`
    )
  }
}
